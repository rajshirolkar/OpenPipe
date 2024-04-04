import AWS from "aws-sdk";
import { v4 as uuidv4 } from "uuid";
import { type Readable } from "stream";
import { env } from "~/env.mjs";
import { inverseDatePrefix } from "./utils";

// Initialize the S3 client
const s3 = new AWS.S3({
  accessKeyId: env.AWS_ACCESS_KEY_ID,
  secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
  region: env.AWS_REGION,
});

const bucketName = env.AWS_S3_BUCKET_NAME;

export const initiateMultipartUploadToS3 = async (key: string) => {
  const params = {
    Bucket: bucketName as string,
    Key: key,
  };

  try {
    const multipartUpload = await s3.createMultipartUpload(params).promise();
    return multipartUpload.UploadId;
  } catch (error) {
    console.error("Error initiating multipart upload to S3:", error);
    throw error;
  }
};

export const generatePresignedUploadUrlForPart = async (
  key: string,
  partNumber: number,
  uploadId: string,
) => {
  const params = {
    Bucket: bucketName,
    Key: key,
    PartNumber: partNumber,
    UploadId: uploadId,
    Expires: 600, // Presigned URL expiration time in seconds
  };

  try {
    const presignedUploadUrl = await s3.getSignedUrlPromise("uploadPart", params);
    return presignedUploadUrl;
  } catch (error) {
    console.error("Error generating presigned upload URL for part:", error);
    throw error;
  }
};

export const completeMultipartUploadToS3 = async (
  key: string,
  uploadId: string,
  parts: Array<{ ETag: string; PartNumber: number }>,
) => {
  const params = {
    Bucket: bucketName as string,
    Key: key,
    UploadId: uploadId,
    MultipartUpload: {
      Parts: parts,
    },
  };

  try {
    const completedUpload = await s3.completeMultipartUpload(params).promise();
    console.log("Completed multipart upload to S3:", completedUpload);

    return {
      bucket: completedUpload.Bucket,
      key: completedUpload.Key,
      location: completedUpload.Location,
      ETag: completedUpload.ETag,
    };
  } catch (error) {
    console.error("Error completing multipart upload to S3:", error);
    throw error;
  }
};

export const abortMultipartUploadToS3 = async (key: string, uploadId: string) => {
  const params = {
    Bucket: bucketName as string,
    Key: key,
    UploadId: uploadId,
  };

  try {
    await s3.abortMultipartUpload(params).promise();
    console.log(`Multipart upload aborted for key: ${key}`);
  } catch (error) {
    console.error("Error aborting multipart upload to S3:", error);
    throw error;
  }
};

// Upload a JSONL file stream to S3
export const uploadJsonlToS3 = async (stream: Readable) => {
  const key = `${inverseDatePrefix()}-${uuidv4()}-training.jsonl`;

  const params = {
    Bucket: bucketName as string,
    Key: key,
    Body: stream,
    ContentType: "application/jsonl",
  };

  await s3.upload(params).promise();

  return key;
};

//generate a presigned URL for downloading
export const generatePresignedDownloadUrl = async (key: string) => {
  const params = {
    Bucket: bucketName,
    Key: key,
    Expires: 600,
  };

  return s3.getSignedUrlPromise("getObject", params);
};

export async function downloadBlobToStringsFromS3({
  blobName,
  maxEntriesToImport,
  onProgress,
  chunkInterval,
}: {
  blobName: string;
  maxEntriesToImport: number;
  onProgress: (progress: number) => Promise<void>;
  chunkInterval?: number;
}) {
  const params = {
    Bucket: bucketName as string,
    Key: blobName,
  };

  const stream = s3.getObject(params).createReadStream();

  stream.on("error", (err) => {
    console.error("Error downloading blob from S3", err);
  });

  return streamToNdStrings({
    readableStream: stream,
    maxEntriesToImport: maxEntriesToImport * 2, // Account for up to 50% errored lines
    onProgress,
    chunkInterval,
  });
}

async function streamToNdStrings({
  readableStream,
  maxEntriesToImport,
  onProgress,
  chunkInterval = 1048576, // send progress every 1MB
}: {
  readableStream: NodeJS.ReadableStream;
  maxEntriesToImport: number;
  onProgress?: (progress: number) => Promise<void>;
  chunkInterval?: number;
}): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const lines: string[] = [];
    let bytesDownloaded = 0;
    let lastReportedByteCount = 0;
    let tempBuffer: Buffer = Buffer.alloc(0);
    let numEntriesImported = 0;

    readableStream.on("data", (chunk: Buffer) => {
      bytesDownloaded += chunk.byteLength;

      // Report progress
      if (onProgress && bytesDownloaded - lastReportedByteCount >= chunkInterval) {
        void onProgress(bytesDownloaded);
        lastReportedByteCount = bytesDownloaded;
      }

      // Combine with leftover buffer from previous chunk
      chunk = Buffer.concat([tempBuffer, chunk]);

      let newlineIndex;
      while (
        (newlineIndex = chunk.indexOf(0x0a)) !== -1 &&
        numEntriesImported < maxEntriesToImport
      ) {
        const line = chunk.slice(0, newlineIndex).toString("utf-8");
        lines.push(line);
        chunk = chunk.slice(newlineIndex + 1);
        numEntriesImported++;
      }

      if (numEntriesImported >= maxEntriesToImport) {
        // TODO: cancel the stream
        resolve(lines);
        return;
      }

      // Save leftover data for next chunk
      tempBuffer = chunk;
    });

    readableStream.on("end", () => {
      if (tempBuffer.length > 0) {
        lines.push(tempBuffer.toString("utf-8")); // add the last part
      }

      resolve(lines);
    });

    readableStream.on("error", reject);
  });
}
