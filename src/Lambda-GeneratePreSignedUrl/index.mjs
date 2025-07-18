import {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import pkg from "@aws-sdk/lib-dynamodb";
const { DynamoDBDocumentClient, ScanCommand } = pkg;
import { v4 as uuidv4 } from "uuid";

const s3Client = new S3Client({ region: "us-east-1" });
const dynamoDBClient = new DynamoDBClient({ region: "ca-central-1" });
const dynamoDB = DynamoDBDocumentClient.from(dynamoDBClient);

export const handler = async (event) => {
  try {
    // Log the full event for debugging
    console.log("Full event:", JSON.stringify(event, null, 2));

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
          "Access-Control-Allow-Headers": "Content-Type",
          "Access-Control-Allow-Methods": "POST,OPTIONS",
        },
        body: JSON.stringify({ error: "Invalid JSON in request body." }),
      };
    }

    const { fileKey, serialKey, email } = body;

    // Log received body
    console.log("Received request body:", { fileKey, serialKey, email });

    // Validate input
    const missingFields = [];
    if (!fileKey?.trim()) missingFields.push("fileKey");
    if (!serialKey?.trim()) missingFields.push("serialKey");
    if (!email?.trim()) missingFields.push("email");

    if (missingFields.length > 0) {
      console.error("Missing or empty fields:", missingFields.join(", "));
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type",
          "Access-Control-Allow-Methods": "POST,OPTIONS",
        },
        body: JSON.stringify({
          error: `Missing or empty fields: ${missingFields.join(", ")}`,
          received: { fileKey, serialKey, email },
        }),
      };
    }

    // Normalize serial key to match VerifySerialKey logic
    const normalizedSerialKey = serialKey.replace(/\s+/g, "\n").trim();
    const serialKeyVariations = [
      normalizedSerialKey,
      normalizedSerialKey + "\n",
      normalizedSerialKey + "\n\n",
      normalizedSerialKey.replace(/\n/g, ""),
    ];

    let customer;
    // Try each serial key variation
    for (const key of serialKeyVariations) {
      const params = {
        TableName: "CustomerTable",
        FilterExpression: "contains(serialKey, :serialKey)",
        ExpressionAttributeValues: {
          ":serialKey": key,
        },
      };

      try {
        const result = await dynamoDB.send(new ScanCommand(params));
        console.log(
          "DynamoDB scan result for key:",
          key,
          "Items:",
          result.Items
        );

        if (result.Items && result.Items.length > 0) {
          // Check ownerData for email match
          const matchingItem = result.Items.find((item) => {
            let ownerData = [];
            try {
              ownerData = Array.isArray(item.ownerData)
                ? item.ownerData
                : typeof item.ownerData === "string" && item.ownerData
                ? JSON.parse(item.ownerData)
                : [];
              if (!Array.isArray(ownerData)) ownerData = [];
            } catch (e) {
              console.error("Error parsing ownerData:", item, e.message);
              return false;
            }
            console.log("ownerData:", ownerData, "Checking email:", email);
            return ownerData.some(
              (owner) =>
                owner.email && owner.email.toLowerCase() === email.toLowerCase()
            );
          });

          if (matchingItem) {
            customer = matchingItem;
            console.log(
              "Found matching customer with serialKey:",
              key,
              "and email:",
              email
            );
            break;
          }
        }
      } catch (e) {
        console.error("DynamoDB scan failed for key:", key, e.message, e.stack);
        continue;
      }
    }

    if (!customer) {
      console.error(
        "Customer not found for email:",
        email,
        "and serialKey variations:",
        serialKeyVariations
      );
      return {
        statusCode: 404,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type",
          "Access-Control-Allow-Methods": "POST,OPTIONS",
        },
        body: JSON.stringify({ error: "Customer not found." }),
      };
    }

    // Check if already downloaded
    if (customer.DownloadedFounderSeriesSUN) {
      console.log("Customer already downloaded SUN:", normalizedSerialKey);
      return {
        statusCode: 403,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type",
          "Access-Control-Allow-Methods": "POST,OPTIONS",
        },
        body: JSON.stringify({
          error: "You have already downloaded your SUN.",
          hasDownloaded: true,
        }),
      };
    }

    // Validate fileKey format (e.g., F 0001.zip)
    if (!fileKey.match(/^F \d{4}\.zip$/)) {
      console.error("Invalid fileKey format:", fileKey);
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type",
          "Access-Control-Allow-Methods": "POST,OPTIONS",
        },
        body: JSON.stringify({ error: "Invalid file key format." }),
      };
    }

    // Construct S3 key using the provided fileKey
    const s3Key = `Compressed test SUN's2/${fileKey}`;

    // Check if file exists in S3
    try {
      const headCommand = new HeadObjectCommand({
        Bucket: "my-bucket-founder-series-sun",
        Key: s3Key,
      });
      await s3Client.send(headCommand);
      console.log("File exists in S3:", s3Key);
    } catch (e) {
      console.error();

      if (e.name === "NotFound" || e.name === "NoSuchKey") {
        console.log("File not found in S3, SUN under construction:", s3Key);
        return {
          statusCode: 202, // Changed to 202 to indicate an informational status
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "POST,OPTIONS",
          },
          body: JSON.stringify({
            error: "SUN Under Construction",
            message:
              "Your SUN is under construction. We will notify you when it’s ready to download.",
          }),
        };
      }
      throw e; // Rethrow other errors
    }

    // Generate pre-signed URL
    const command = new GetObjectCommand({
      Bucket: "my-bucket-founder-series-sun",
      Key: s3Key,
    });

    let url;
    const downloadId = uuidv4();
    try {
      url = await getSignedUrl(s3Client, command, { expiresIn: 3600 }); // URL expires in 1 hour
      console.log("Generated pre-signed URL for downloadId:", downloadId);
    } catch (e) {
      console.error("Error generating pre-signed URL:", e.message, e.stack);
      return {
        statusCode: 500,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type",
          "Access-Control-Allow-Methods": "POST,OPTIONS",
        },
        body: JSON.stringify({ error: "Failed to generate download URL." }),
      };
    }

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "POST,OPTIONS",
      },
      body: JSON.stringify({ url, downloadId, hasDownloaded: false }),
    };
  } catch (error) {
    console.error("Error in generatePresignedUrl:", error.message, error.stack);
    return {
      statusCode: 500,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "POST,OPTIONS",
      },
      body: JSON.stringify({ error: "Internal server error." }),
    };
  }
};
