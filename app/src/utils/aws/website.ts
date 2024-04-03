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

  // Step 1: Initiate multipart upload to get the upload ID
  const { uploadId } = await api.client.datasets.initiateMultipartUpload.query({
    projectId,
    filename: blobName,
  });

  // Step 2: Split the file into parts and get presigned URLs for each part
  const PART_SIZE = 5 * 1024 * 1024; // 5MB for each part
  const fileSize = file.size;
  const NUM_PARTS = Math.ceil(fileSize / PART_SIZE);
  const partUploadPromises = [];

  for (let partNumber = 1; partNumber <= NUM_PARTS; partNumber++) {
    const start = (partNumber - 1) * PART_SIZE;
    const end = Math.min(start + PART_SIZE, fileSize);
    const part = file.slice(start, end);

    // Get a presigned URL for this part
    const { presignedUploadUrl } = await api.client.datasets.getPresignedUploadUrlForPart.query({
      projectId,
      filename: blobName,
      partNumber,
      uploadId: uploadId as string,
    });

    // Step 3: Upload the part
    const uploadPartPromise = fetch(presignedUploadUrl, {
      method: "PUT",
      headers: {
        "Content-Type": "application/jsonl",
      },
      body: part,
    }).then((response) => {
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      console.log(`Part ${partNumber} uploaded successfully`);

      return response.headers.get("ETag");
    });

    partUploadPromises.push(uploadPartPromise);
  }

  try {
    // Wait for all parts to be uploaded
    const etags = await Promise.all(partUploadPromises);
    const parts = etags.map((etag, index) => {
      if (etag === null) {
        throw new Error(`ETag is null for part number ${index + 1}`);
      }
      return { ETag: etag, PartNumber: index + 1 };
    });

    // Step 4: Complete the multipart upload
    await api.client.datasets.completeMultipartUpload.query({
      projectId,
      filename: blobName,
      uploadId: uploadId as string,
      parts,
    });

    return blobName;
  } catch (error) {
    console.error("Error uploading file to S3", error);

    // Abort the multipart upload on error
    try {
      await api.client.datasets.abortMultipartUpload.mutate({
        projectId,
        filename: blobName,
        uploadId: uploadId as string,
      });
      console.log("Multipart upload aborted due to error.");
    } catch (abortError) {
      console.error("Failed to abort multipart upload:", abortError);
    }

    throw error; // Rethrow the original error
  }
};
