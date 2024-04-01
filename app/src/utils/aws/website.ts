import { useAppStore } from "~/state/store";
import { v4 as uuidv4 } from "uuid";
import { inverseDatePrefix } from "~/utils/aws/utils";

export const uploadDatasetEntryFileToS3 = async (projectId: string, file: File) => {
  const { api } = useAppStore.getState();

  if (!api) throw Error("api not initialized");

  // base name without extension
  const basename = file.name.split("/").pop()?.split(".").shift();
  if (!basename) throw Error("basename not found");

  const blobName = `${inverseDatePrefix()}-${basename}-${uuidv4()}-uploaded.jsonl`;

  const { presignedUploadUrl, key } = await api.client.datasets.getPresignedUploadUrl.query({
    projectId,
    filename: blobName,
  });

  const response = await fetch(presignedUploadUrl, {
    method: "PUT",
    headers: {
      "Content-Type": "application/jsonl",
    },
    body: file,
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return key;
};
