const {
  DynamoDBClient,
  BatchWriteItemCommand,
} = require("@aws-sdk/client-dynamodb");

const dynamoDBClient = new DynamoDBClient({ region: "ca-central-1" });

const batchWriteItems = async (items) => {
  const params = {
    RequestItems: {
      FrameworkCcubeTable: items,
    },
  };

  const command = new BatchWriteItemCommand(params);

  try {
    await dynamoDBClient.send(command);
  } catch (error) {
    console.error("Error during batch write:", error);
    throw error; // Re-throw to handle it in the caller function.
  }
};

const insertData = async () => {
  console.log("The data insertion has started.");
  const items = [];

  for (let z = 1; z <= 100; z++) {
    for (let y = 1; y <= 100; y++) {
      for (let x = 1; x <= 100; x++) {
        const coordinate = `x${x},y${y},z${z}`;

        items.push({
          PutRequest: { Item: { Coordinate: { S: coordinate } } },
        });

        // If we reach 25 items (max for a single batch write), send them
        if (items.length === 25) {
          await writeWithRetry(items);
          items.length = 0; // Clear the array for the next batch
        }
      }
    }
  }

  // Write any remaining items
  if (items.length > 0) {
    await writeWithRetry(items);
  }

  console.log("Data insertion complete.");
};

const writeWithRetry = async (items, retries = 3) => {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      await batchWriteItems(items);
      return; // Exit if the write is successful
    } catch (error) {
      console.error(`Attempt ${attempt + 1} failed:`, error);
      if (attempt === retries - 1) {
        console.error("Max retries reached. Failed to write items:", items);
        throw error; // Re-throw if max retries reached
      }
      // Implement a delay before retrying
      await new Promise((resolve) => setTimeout(resolve, 1000 * (attempt + 1)));
    }
  }
};

insertData().catch((err) => console.error("Error inserting data:", err));
