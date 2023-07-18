import dedent from "dedent";
import { isObject } from "lodash-es";
import { z } from "zod";
import { createTRPCRouter, publicProcedure } from "~/server/api/trpc";
import { prisma } from "~/server/db";
import { generateNewCell } from "~/server/utils/generateNewCell";
import { OpenAIChatModel } from "~/server/types";
import { constructPrompt } from "~/server/utils/constructPrompt";
import userError from "~/server/utils/error";
import { recordExperimentUpdated } from "~/server/utils/recordExperimentUpdated";
import { calculateTokenCost } from "~/utils/calculateTokenCost";
import { reorderPromptVariants } from "~/server/utils/reorderPromptVariants";
import { type PromptVariant } from "@prisma/client";

export const promptVariantsRouter = createTRPCRouter({
  list: publicProcedure.input(z.object({ experimentId: z.string() })).query(async ({ input }) => {
    return await prisma.promptVariant.findMany({
      where: {
        experimentId: input.experimentId,
        visible: true,
      },
      orderBy: { sortIndex: "asc" },
    });
  }),

  stats: publicProcedure.input(z.object({ variantId: z.string() })).query(async ({ input }) => {
    const variant = await prisma.promptVariant.findUnique({
      where: {
        id: input.variantId,
      },
    });

    if (!variant) {
      throw new Error(`Prompt Variant with id ${input.variantId} does not exist`);
    }

    const outputEvals = await prisma.outputEvaluation.groupBy({
      by: ["evaluationId"],
      _sum: {
        result: true,
      },
      _count: {
        id: true,
      },
      where: {
        modelOutput: {
          scenarioVariantCell: {
            promptVariant: {
              id: input.variantId,
              visible: true,
            },
            testScenario: {
              visible: true,
            },
          },
        },
      },
    });

    const evals = await prisma.evaluation.findMany({
      where: {
        experimentId: variant.experimentId,
      },
    });

    const evalResults = evals.map((evalItem) => {
      const evalResult = outputEvals.find((outputEval) => outputEval.evaluationId === evalItem.id);
      return {
        id: evalItem.id,
        label: evalItem.label,
        passCount: evalResult?._sum?.result ?? 0,
        totalCount: evalResult?._count?.id ?? 1,
      };
    });

    const scenarioCount = await prisma.testScenario.count({
      where: {
        experimentId: variant.experimentId,
        visible: true,
      },
    });
    const outputCount = await prisma.scenarioVariantCell.count({
      where: {
        promptVariantId: input.variantId,
        testScenario: { visible: true },
        modelOutput: {
          is: {},
        },
      },
    });

    const overallTokens = await prisma.modelOutput.aggregate({
      where: {
        scenarioVariantCell: {
          promptVariantId: input.variantId,
          testScenario: {
            visible: true,
          },
        },
      },
      _sum: {
        promptTokens: true,
        completionTokens: true,
      },
    });

    const promptTokens = overallTokens._sum?.promptTokens ?? 0;
    const overallPromptCost = calculateTokenCost(variant.model, promptTokens);
    const completionTokens = overallTokens._sum?.completionTokens ?? 0;
    const overallCompletionCost = calculateTokenCost(variant.model, completionTokens, true);

    const overallCost = overallPromptCost + overallCompletionCost;

    const awaitingRetrievals = !!(await prisma.scenarioVariantCell.findFirst({
      where: {
        promptVariantId: input.variantId,
        testScenario: { visible: true },
        // Check if is PENDING or IN_PROGRESS
        retrievalStatus: {
          in: ["PENDING", "IN_PROGRESS"],
        },
      },
    }));

    return {
      evalResults,
      promptTokens,
      completionTokens,
      overallCost,
      scenarioCount,
      outputCount,
      awaitingRetrievals,
    };
  }),

  create: publicProcedure
    .input(
      z.object({
        experimentId: z.string(),
        variantId: z.string().optional(),
      }),
    )
    .mutation(async ({ input }) => {
      let originalVariant: PromptVariant | null = null;
      if (input.variantId) {
        originalVariant = await prisma.promptVariant.findUnique({
          where: {
            id: input.variantId,
          },
        });
      } else {
        originalVariant = await prisma.promptVariant.findFirst({
          where: {
            experimentId: input.experimentId,
            visible: true,
          },
          orderBy: {
            sortIndex: "desc",
          },
        });
      }

      const largestSortIndex =
        (
          await prisma.promptVariant.aggregate({
            where: {
              experimentId: input.experimentId,
            },
            _max: {
              sortIndex: true,
            },
          })
        )._max?.sortIndex ?? 0;

      const newVariantLabel =
        input.variantId && originalVariant
          ? `${originalVariant?.label} Copy`
          : `Prompt Variant ${largestSortIndex + 2}`;

      const createNewVariantAction = prisma.promptVariant.create({
        data: {
          experimentId: input.experimentId,
          label: newVariantLabel,
          sortIndex: (originalVariant?.sortIndex ?? 0) + 1,
          constructFn:
            originalVariant?.constructFn ??
            dedent`
          prompt = {
            model: "gpt-3.5-turbo",
            messages: [
              {
                role: "system",
                content: "Return 'Hello, world!'",
              }
            ]
          }`,
          model: originalVariant?.model ?? "gpt-3.5-turbo",
        },
      });

      const [newVariant] = await prisma.$transaction([
        createNewVariantAction,
        recordExperimentUpdated(input.experimentId),
      ]);

      if (originalVariant) {
        // Insert new variant to right of original variant
        await reorderPromptVariants(newVariant.id, originalVariant.id, true);
      }

      const scenarios = await prisma.testScenario.findMany({
        where: {
          experimentId: input.experimentId,
          visible: true,
        },
      });

      for (const scenario of scenarios) {
        await generateNewCell(newVariant.id, scenario.id);
      }

      return newVariant;
    }),

  update: publicProcedure
    .input(
      z.object({
        id: z.string(),
        updates: z.object({
          label: z.string().optional(),
        }),
      }),
    )
    .mutation(async ({ input }) => {
      const existing = await prisma.promptVariant.findUnique({
        where: {
          id: input.id,
        },
      });

      if (!existing) {
        throw new Error(`Prompt Variant with id ${input.id} does not exist`);
      }

      const updatePromptVariantAction = prisma.promptVariant.update({
        where: {
          id: input.id,
        },
        data: input.updates,
      });

      const [updatedPromptVariant] = await prisma.$transaction([
        updatePromptVariantAction,
        recordExperimentUpdated(existing.experimentId),
      ]);

      return updatedPromptVariant;
    }),

  hide: publicProcedure
    .input(
      z.object({
        id: z.string(),
      }),
    )
    .mutation(async ({ input }) => {
      const updatedPromptVariant = await prisma.promptVariant.update({
        where: { id: input.id },
        data: { visible: false, experiment: { update: { updatedAt: new Date() } } },
      });

      return updatedPromptVariant;
    }),

  replaceVariant: publicProcedure
    .input(
      z.object({
        id: z.string(),
        constructFn: z.string(),
      }),
    )
    .mutation(async ({ input }) => {
      const existing = await prisma.promptVariant.findUnique({
        where: {
          id: input.id,
        },
      });

      if (!existing) {
        throw new Error(`Prompt Variant with id ${input.id} does not exist`);
      }

      let model = existing.model;
      try {
        const contructedPrompt = await constructPrompt({ constructFn: input.constructFn }, null);

        if (!isObject(contructedPrompt)) {
          return userError("Prompt is not an object");
        }
        if (!("model" in contructedPrompt)) {
          return userError("Prompt does not define a model");
        }
        if (
          typeof contructedPrompt.model !== "string" ||
          !(contructedPrompt.model in OpenAIChatModel)
        ) {
          return userError("Prompt defines an invalid model");
        }
        model = contructedPrompt.model;
      } catch (e) {
        return userError((e as Error).message);
      }

      // Create a duplicate with only the config changed
      const newVariant = await prisma.promptVariant.create({
        data: {
          experimentId: existing.experimentId,
          label: existing.label,
          sortIndex: existing.sortIndex,
          uiId: existing.uiId,
          constructFn: input.constructFn,
          model,
        },
      });

      // Hide anything with the same uiId besides the new one
      const hideOldVariants = prisma.promptVariant.updateMany({
        where: {
          uiId: existing.uiId,
          id: {
            not: newVariant.id,
          },
        },
        data: {
          visible: false,
        },
      });

      await prisma.$transaction([hideOldVariants, recordExperimentUpdated(existing.experimentId)]);

      const scenarios = await prisma.testScenario.findMany({
        where: {
          experimentId: newVariant.experimentId,
          visible: true,
        },
      });

      for (const scenario of scenarios) {
        await generateNewCell(newVariant.id, scenario.id);
      }

      return { status: "ok" } as const;
    }),

  reorder: publicProcedure
    .input(
      z.object({
        draggedId: z.string(),
        droppedId: z.string(),
      }),
    )
    .mutation(async ({ input }) => {
      await reorderPromptVariants(input.draggedId, input.droppedId);
    }),
});
