import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const s3Client = new S3Client({ region: "us-east-1" });

export const handler = async (event) => {
  try {
    // Parse request body
    let body;
    try {
      body = JSON.parse(event.body || "{}");
    } catch (e) {
      console.error("Invalid JSON:", e.message);
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Invalid JSON in request body." }),
      };
    }

    const { fileKey } = body;

    // Validate input
    if (!fileKey?.trim()) {
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "File key is required." }),
      };
    }

    // Extract serial number (e.g., F0000 from F7410347907421F0000473272182.zip)
    const serialNumberMatch = fileKey.match(/F\d{4}/);
    if (!serialNumberMatch) {
      console.error("Invalid fileKey format:", fileKey);
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Invalid file key format." }),
      };
    }

    const serialNumber = serialNumberMatch[0];
    // Include space in the S3 key (e.g., F 0000.zip)
    const s3Key = `Compressed test SUN's2/${serialNumber.replace(
      /F(\d{4})/,
      "F $1"
    )}.zip`;

    // Generate pre-signed URL
    const command = new GetObjectCommand({
      Bucket: "my-bucket-founder-series-sun",
      Key: s3Key,
    });

    try {
      const url = await getSignedUrl(s3Client, command, { expiresIn: 3600 }); // URL expires in 1 hour
      return {
        statusCode: 200,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ url }),
      };
    } catch (error) {
      console.error(
        "Error generating pre-signed URL:",
        error.message,
        error.stack
      );
      if (error.name === "NoSuchKey") {
        return {
          statusCode: 404,
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
          body: JSON.stringify({ error: `File ${s3Key} not found in S3.` }),
        };
      }
      return {
        statusCode: 500,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Failed to generate download URL." }),
      };
    }
  } catch (error) {
    console.error("Error in generatePresignedUrl:", error.message, error.stack);
    return {
      statusCode: 500,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({ error: "Internal server error." }),
    };
  }
};
