const AWS = require("aws-sdk");
const dynamoDB = new AWS.DynamoDB.DocumentClient({ region: "ca-central-1" });

exports.handler = async (event) => {
  try {
    const { serialKey, email } = JSON.parse(event.body);

    // Validate input
    if (!serialKey || !email) {
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Serial key and email are required." }),
      };
    }

    // Query DynamoDB for the serial key
    const params = {
      TableName: "CustomerTable",
      FilterExpression: "serialKey = :serialKey",
      ExpressionAttributeValues: {
        ":serialKey": serialKey.trim(),
      },
    };

    const result = await dynamoDB.scan(params).promise();

    if (!result.Items || result.Items.length === 0) {
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Invalid serial key." }),
      };
    }

    const item = result.Items[0];

    // Parse serial keys to match PurchaseTP format
    const serialKeys = item.serialKey
      .split("\n\n,")
      .filter((key) => key.trim());
    if (!serialKeys.includes(serialKey.trim())) {
      return {
        statusCode: 400,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Serial key does not match." }),
      };
    }

    // Generate fileKey
    const fileKey = `F${serialKeys[1]?.slice(1) || serialKey.slice(1)}.zip`;

    // Validate email against ownerData
    let ownerData = [];
    try {
      ownerData = Array.isArray(item.ownerData)
        ? item.ownerData
        : JSON.parse(item.ownerData || "[]");
    } catch (e) {
      console.error("Failed to parse ownerData:", e);
    }

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

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({ fileKey }),
    };
  } catch (error) {
    console.error("Error in verifySerialKey:", error);
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
