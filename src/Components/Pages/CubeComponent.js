const numbersString = require("./Cubenumbers"); // Ensure this path is correct
const {
  DynamoDBClient,
  BatchWriteItemCommand,
} = require("@aws-sdk/client-dynamodb");

const numbersArray = numbersString.split(" ").map((num) => num.trim());
const dynamoDBClient = new DynamoDBClient({ region: "ca-central-1" }); // Set your AWS region here

const batchWriteItems = async (items) => {
  const params = {
    RequestItems: {
      CoordinatesTable: items, // Ensure your table name is correct
    },
  };

  const command = new BatchWriteItemCommand(params); // Create the command here
  return await dynamoDBClient.send(command); // Send the command using the client
};

const insertData = async () => {
  const items = [];
  let index = 0;

  for (let x = 1; x <= 100; x++) {
    for (let y = 1; y <= 100; y++) {
      for (let z = 1; z <= 100; z++) {
        if (index >= numbersArray.length) break; // Prevent going out of bounds
        const coordinate = `x${x},y${y},z${z}`;
        const number = numbersArray[index];

        items.push({
          PutRequest: {
            Item: {
              Coordinate: { S: coordinate },
              Number: { N: number },
            },
          },
        });

        index++;

        // If we reach 25 items (max for a single batch write), send them
        if (items.length === 25) {
          await batchWriteItems(items);
          items.length = 0; // Clear the array for the next batch
        }
      }
      if (index >= numbersArray.length) break; // Prevent going out of bounds
    }
    if (index >= numbersArray.length) break; // Prevent going out of bounds
  }

  // Write any remaining items
  if (items.length > 0) {
    await batchWriteItems(items);
  }

  console.log("Data insertion complete.");
};

insertData().catch((err) => console.error("Error inserting data:", err));
