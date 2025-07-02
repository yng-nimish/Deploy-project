import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import pkg from "@aws-sdk/lib-dynamodb";
const { DynamoDBDocumentClient, ScanCommand, UpdateCommand, QueryCommand } =
  pkg;

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

    const { downloadId, serialKey, email } = body;

    // Log received body
    console.log("Received confirmation request:", {
      downloadId,
      serialKey,
      email,
    });

    // Validate input
    const missingFields = [];
    if (!downloadId?.trim()) missingFields.push("downloadId");
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
          received: { downloadId, serialKey, email },
        }),
      };
    }

    // Normalize serial key
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
          "DynamoDB Scan result for key:",
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
            console.log(
              "Parsed ownerData:",
              ownerData,
              "Checking email:",
              email
            );
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
        console.error("DynamoDB Scan failed for key:", key, e.message, e.stack);
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
    if (customer.DownloadedFounderSeriesSUN === true) {
      console.log("Customer already downloaded SUN:", normalizedSerialKey);
      return {
        statusCode: 200,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type",
          "Access-Control-Allow-Methods": "POST,OPTIONS",
        },
        body: JSON.stringify({ message: "Download already confirmed." }),
      };
    }

    // Update DynamoDB to set DownloadedFounderSeriesSUN to true
    try {
      const updateCommand = new UpdateCommand({
        TableName: "CustomerTable",
        Key: {
          id: customer.id,
        },
        UpdateExpression: "SET DownloadedFounderSeriesSUN = :val",
        ExpressionAttributeValues: {
          ":val": true,
          ":false": false,
        },
        ConditionExpression:
          "attribute_not_exists(DownloadedFounderSeriesSUN) OR DownloadedFounderSeriesSUN = :false",
      });
      await dynamoDB.send(updateCommand);
      console.log(
        "Updated DownloadedFounderSeriesSUN to true for:",
        normalizedSerialKey,
        "downloadId:",
        downloadId
      );
    } catch (e) {
      console.error("Error updating DynamoDB:", e.message, e.stack);
      return {
        statusCode: 500,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type",
          "Access-Control-Allow-Methods": "POST,OPTIONS",
        },
        body: JSON.stringify({ error: "Failed to confirm download." }),
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
      body: JSON.stringify({ message: "Download confirmed successfully." }),
    };
  } catch (error) {
    console.error("Error in confirmDownload:", error.message, error.stack);
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
