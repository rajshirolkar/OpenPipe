import { type Node } from "@prisma/client";
import { sql } from "kysely";

import { kysely } from "~/server/db";
import { type typedNode } from "~/server/utils/nodes/node.types";
import { nodePropertiesByType } from "./processNode.task";

export const updateCached = async ({
  node,
}: {
  node: ReturnType<typeof typedNode> & Pick<Node, "id" | "projectId" | "hash">;
}) => {
  const nodeProperties = nodePropertiesByType[node.type];

  if (nodeProperties.cacheMatchFields && nodeProperties.cacheWriteFields) {
    let processCachedQuery = kysely
      .updateTable("NodeEntry as ne")
      .set({ status: "PROCESSED" })
      .from("CachedProcessedEntry as cpne")
      .where((eb) => eb.or([eb("cpne.nodeHash", "=", node.hash), eb("cpne.nodeId", "=", node.id)]))
      .where("ne.nodeId", "=", node.id)
      .where("ne.status", "=", "PENDING");

    if (nodeProperties.cacheWriteFields.includes("outgoingInputHash")) {
      processCachedQuery = processCachedQuery.set({
        inputHash: sql`"cpne"."outgoingInputHash"`,
      });
    }
    if (nodeProperties.cacheWriteFields.includes("outgoingOutputHash")) {
      processCachedQuery = processCachedQuery.set({
        outputHash: sql`"cpne"."outgoingOutputHash"`,
        originalOutputHash: sql`"ne"."outputHash"`,
      });
    }
    if (nodeProperties.cacheWriteFields.includes("outgoingSplit")) {
      processCachedQuery = processCachedQuery.set({ split: sql`"cpne"."outgoingSplit"` });
    }
    if (nodeProperties.cacheMatchFields.includes("nodeEntryPersistentId")) {
      processCachedQuery = processCachedQuery.whereRef(
        "ne.persistentId",
        "=",
        "cpne.nodeEntryPersistentId",
      );
    }
    if (nodeProperties.cacheMatchFields.includes("incomingInputHash")) {
      processCachedQuery = processCachedQuery.whereRef(
        "ne.inputHash",
        "=",
        "cpne.incomingInputHash",
      );
    }
    if (nodeProperties.cacheMatchFields.includes("incomingOutputHash")) {
      processCachedQuery = processCachedQuery.whereRef(
        "ne.outputHash",
        "=",
        "cpne.incomingOutputHash",
      );
    }
    await processCachedQuery.execute();
  }
};
