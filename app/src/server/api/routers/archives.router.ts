import { z } from "zod";
import { sql } from "kysely";

import { createTRPCRouter, protectedProcedure } from "~/server/api/trpc";
import { kysely, prisma } from "~/server/db";
import { requireCanModifyProject, requireCanViewProject } from "~/utils/accessControl";
import { error, success } from "~/utils/errorHandling/standardResponses";
import { relabelOptions, typedNode } from "~/server/utils/nodes/node.types";
import { enqueueProcessNode } from "~/server/tasks/nodes/processNodes/processNode.task";
import { checkNodeInput } from "~/server/utils/nodes/checkNodeInput";
import { getArchives } from "~/server/utils/nodes/relationalQueries";
import { TRPCError } from "@trpc/server";

export const archivesRouter = createTRPCRouter({
  listForDataset: protectedProcedure
    .input(
      z.object({
        datasetId: z.string(),
      }),
    )
    .query(async ({ input, ctx }) => {
      const datasetNode = await kysely
        .selectFrom("Dataset as d")
        .where("d.id", "=", input.datasetId)
        .innerJoin("Node as n", "n.id", "d.nodeId")
        .selectAll("n")
        .executeTakeFirst();

      if (!datasetNode) throw new TRPCError({ code: "NOT_FOUND", message: "Dataset not found" });

      await requireCanViewProject(datasetNode.projectId, ctx);

      const tNode = typedNode(datasetNode);

      if (tNode.type !== "Dataset")
        throw new TRPCError({ code: "BAD_REQUEST", message: "Invalid node type" });

      const archives = await getArchives({
        datasetManualRelabelNodeId: tNode.config.manualRelabelNodeId,
      })
        .leftJoin("DatasetFileUpload as dfu", "dfu.nodeId", "archiveNode.id")
        .where("dfu.errorMessage", "is", null)
        .leftJoin("NodeEntry as nd", "archiveNode.id", "nd.nodeId")
        .groupBy(["archiveNode.id", "llmRelabelNode.id"])
        .distinctOn("archiveNode.createdAt")
        .selectAll("archiveNode")
        .select([
          "llmRelabelNode.id as llmRelabelNodeId",
          "llmRelabelNode.config as llmRelabelNodeConfig",
          sql<number>`SUM(CASE WHEN nd.split = 'TRAIN' THEN 1 ELSE 0 END)::int`.as(
            "numTrainEntries",
          ),
          sql<number>`SUM(CASE WHEN nd.split = 'TEST' THEN 1 ELSE 0 END)::int`.as("numTestEntries"),
        ])
        .orderBy("archiveNode.createdAt", "desc")
        .execute()
        .then((archives) =>
          archives.map((archive) => {
            const typedLLMRelabelNode = typedNode({
              type: "LLMRelabel",
              config: archive.llmRelabelNodeConfig,
            });
            if (typedLLMRelabelNode.type !== "LLMRelabel")
              throw new TRPCError({
                code: "BAD_REQUEST",
                message: "Invalid relabel node configuration",
              });

            return {
              ...archive,
              relabelOption: typedLLMRelabelNode.config.relabelLLM,
            };
          }),
        );

      return archives;
    }),
  updateRelabelingModel: protectedProcedure
    .input(
      z.object({
        archiveLLMRelabelNodeId: z.string(),
        relabelOption: z.enum(relabelOptions),
      }),
    )
    .mutation(async ({ input, ctx }) => {
      const llmRelabelNode = await kysely
        .selectFrom("Node as n")
        .where("n.id", "=", input.archiveLLMRelabelNodeId)
        .selectAll("n")
        .executeTakeFirst();

      if (!llmRelabelNode) return error("Relabeling model not found");

      await requireCanModifyProject(llmRelabelNode.projectId, ctx);

      const tLlmRelabelNode = typedNode(llmRelabelNode);

      if (tLlmRelabelNode.type !== "LLMRelabel") return error("Node incorrect type");

      if (tLlmRelabelNode.config.relabelLLM === input.relabelOption)
        return success("Archive relabeling model already set to this option");

      await prisma.node.update({
        where: { id: tLlmRelabelNode.id },
        data: checkNodeInput({
          ...tLlmRelabelNode,
          config: {
            ...tLlmRelabelNode.config,
            relabelLLM: input.relabelOption,
          },
        }),
      });

      await enqueueProcessNode({
        nodeId: tLlmRelabelNode.id,
        invalidateData: true,
      });

      return success("Archive relabeling model updated");
    }),
});
