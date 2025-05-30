import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, ScanCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({ region: "ca-central-1" });
const dynamoDB = DynamoDBDocumentClient.from(client);

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

    const { serialKey, email } = body;

    // Validate inputs
    if (!serialKey?.trim() || !email?.trim()) {
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Serial key and email are required." }),
      };
    }

    // Validate email format
    const emailRegex = /^[^@]+@[^@]+\.[^@]+$/;
    if (!emailRegex.test(email)) {
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Invalid email format." }),
      };
    }

    // Normalize serial key (try both with and without trailing newlines)
    const normalizedSerialKey = serialKey.replace(/\s+/g, "\n").trim();
    const serialKeyVariations = [
      normalizedSerialKey,
      normalizedSerialKey + "\n",
      normalizedSerialKey + "\n\n",
      normalizedSerialKey.replace(/\n/g, ""), // Also try without newlines
    ];

    let result = { Items: [] };
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
        const scanResult = await dynamoDB.send(new ScanCommand(params));
        if (scanResult.Items && scanResult.Items.length > 0) {
          result = scanResult;
          console.log("Found item with serialKey:", key);
          break;
        }
      } catch (e) {
        console.error("DynamoDB scan failed for key:", key, e.message, e.stack);
        continue;
      }
    }

    if (!result.Items || result.Items.length === 0) {
      console.error(
        "No items found for serialKey variations:",
        serialKeyVariations
      );
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Invalid serial key." }),
      };
    }

    if (result.Items.length > 1) {
      console.error("Multiple items found for serialKey:", normalizedSerialKey);
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Multiple serial keys found." }),
      };
    }

    const item = result.Items[0];

    // Parse ownerData
    let ownerData = [];
    try {
      ownerData = Array.isArray(item.ownerData)
        ? item.ownerData
        : typeof item.ownerData === "string" && item.ownerData
        ? JSON.parse(item.ownerData)
        : [];
    } catch (e) {
      console.error("Failed to parse ownerData:", e.message);
      return {
        statusCode: 500,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({
          error: "Invalid owner data format in database.",
        }),
      };
    }

    // Validate email against ownerData
    const isAuthorized = ownerData.some(
      (owner) =>
        owner.email && owner.email.toLowerCase() === email.toLowerCase()
    );

    if (!isAuthorized) {
      return {
        statusCode: 403,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({
          error: "Email does not match the owner of this serial key.",
        }),
      };
    }

    // Generate fileKey
    const fileKey =
      item.fileKey || `F${normalizedSerialKey.replace(/\n/g, "")}.zip`;

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({ fileKey }),
    };
  } catch (error) {
    console.error("Error in verifySerialKey:", error.message, error.stack);
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
