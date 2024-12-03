const gremlin = require("gremlin");
const { DriverRemoteConnection } = gremlin.driver;
const { Graph } = gremlin.structure;
const numbersString = require("./Cubenumbers");

const numbersArray = numbersString.trim().split(" ").map(Number);
console.log("Step 1 complete");

const connectionString =
  "wss:db-neptune-1-instance-1.czcyiua0ag5g.ca-central-1.neptune.amazonaws.com:8182/gremlin"; // Use wss for WebSocket

const connection = new DriverRemoteConnection(connectionString); // The connection is timing out .
const graph = new Graph();
const g = graph.traversal().withRemote(connection);
console.log("Step 2 complete");
console.log("Starting instertion");

const insertNumbers = async (numbers) => {
  for (let i = 0; i < numbers.length; i++) {
    const number = numbers[i];
    try {
      await g.addV("Number").property("value", number).next();
    } catch (error) {
      console.error(`Error inserting number ${number}:`, error);
    }
  }
};

(async () => {
  try {
    await insertNumbers(numbersArray);
    console.log("All numbers inserted successfully.");
  } catch (error) {
    console.error("Error inserting numbers:", error);
  } finally {
    await connection.close();
  }
})();
const verifyNumbers = async () => {
  const numbers = await g.V().hasLabel("Number").values("value").toList();
  console.log("Inserted Numbers:", numbers);
};

// Call this function after the insertion
(async () => {
  try {
    await insertNumbers(numbersArray);
    console.log("All numbers inserted successfully.");

    // Verify the numbers
    await verifyNumbers();
  } catch (error) {
    console.error("Error inserting numbers:", error);
  } finally {
    await connection.close();
  }
})();
