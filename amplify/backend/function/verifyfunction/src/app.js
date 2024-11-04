/*
Copyright 2017 - 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
    http://aws.amazon.com/apache2.0/
or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.

*/

/* Amplify Params - DO NOT EDIT
	AUTH_WEBSITE332CBEF7_USERPOOLID
	ENV
	REGION
Amplify Params - DO NOT EDIT */
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  QueryCommand,
} = require("@aws-sdk/lib-dynamodb");
const express = require("express");
const bodyParser = require("body-parser");
const awsServerlessExpressMiddleware = require("aws-serverless-express/middleware");
const cors = require("cors");

// declare a new express app
const app = express();
app.use(bodyParser.json());
app.use(cors());
app.use(awsServerlessExpressMiddleware.eventContext());

// Initialize DynamoDB client and document client
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

// Endpoint to verify the serial key
app.post("/verify", async (req, res) => {
  const { serialKey } = req.body;
  const userEmail = req.user.email; // Extracted from your authentication logic
  if (!serialKey) {
    return res.status(400).json({ error: "Serial key is required." });
  }

  try {
    // Query to find the customer with the given serial key
    const params = {
      TableName: "CustomerTable",
      FilterExpression: "contains(serialKey, :serialKey)",
      ExpressionAttributeValues: {
        ":serialKey": serialKey,
      },
    };

    const data = await docClient.send(new QueryCommand(params));

    // Check if any items were returned
    if (data.Items && data.Items.length > 0) {
      // Assuming you have only one item that matches
      const ownerEmail = data.Items[0].ownerEmail; // Access the first item

      // Verify the owner email
      if (ownerEmail !== userEmail) {
        return res
          .status(403)
          .json({ error: "You do not own this serial key" });
      }

      return res.status(200).json({
        success: true,
        message: "Serial key is valid and owned by the user.",
      });
    } else {
      return res.status(404).json({ error: "Serial key not found." });
    }
  } catch (error) {
    console.error("Error verifying serial key:", error);
    return res.status(500).json({ error: "Could not verify serial key" });
  }
});

app.listen(3000, function () {
  console.log("App started");
});

// Export the app object. When executing the application local this does nothing. However,
// to port it to AWS Lambda we will create a wrapper around that will load the app from
// this file
module.exports = app;
