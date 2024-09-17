/*
Copyright 2017 - 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
    http://aws.amazon.com/apache2.0/
or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
*/

/* Amplify Params - DO NOT EDIT
	ENV
	REGION
	API_API47CCEB23_APINAME
	API_API47CCEB23_APIID
	API_APIDA2D6043_APINAME
	API_APIDA2D6043_APIID
	API_APIF48D1043_APINAME
	API_APIF48D1043_APIID
	AUTH_WEBSITE332CBEF7_USERPOOLID
	FUNCTION_LAMBDA1_NAME
	FUNCTION_LAMBDA2_NAME
	FUNCTION_WEBSITEC6750C14_NAME
	stripe_key
Amplify Params - DO NOT EDIT */

const aws = require("aws-sdk");

const express = require("express");
const bodyParser = require("body-parser");
const awsServerlessExpressMiddleware = require("aws-serverless-express/middleware");
const stripeLib = require("stripe");

// declare a new express app
const app = express();
app.use(
  bodyParser.json({
    verify: function (req, res, buf) {
      req.rawBody = buf.toString();
    },
  })
);

app.use(awsServerlessExpressMiddleware.eventContext());
const getStripeKey = async () => {
  console.log("Retrieving Stripe key from SSM...");
  const { Parameters } = await new aws.SSM()
    .getParameters({
      Names: ["stripe_key"].map((secretName) => process.env[secretName]),
      WithDecryption: true,
    })
    .promise();
  console.log("Stripe key retrieved successfully.");
  return Parameters[0].Value;
};

// Enable CORS for all methods
app.use(function (req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  next();
});

app.post("/webhook", async (req, res) => {
  console.log("Received request body:", req.body);
  try {
    const stripeKey = await getStripeKey();
    const stripe = stripeLib(stripeKey);
    const items = req.body.items;
    console.log("Processing items:", items);
    let lineItems = [];
    items.forEach((item) => {
      lineItems.push({
        price: item.id,
        quantity: item.quantity,
      });
    });
    console.log("Line items:", lineItems);

    const session = await stripe.checkout.sessions.create({
      line_items: lineItems,
      mode: "payment",
      success_url: "http://yournumberguaranteed.com/purchase",
      cancel_url: "http://yournumberguaranteed.com/career",
    });
    console.log("Stripe session created:", session);

    res.json({ url: session.url });
  } catch (error) {
    console.error("Error creating checkout session:", error);
    res.status(500).send("Internal Server Error");
  }
});

app.listen(3000, function () {
  console.log("App started");
});

// Export the app object. When executing the application local this does nothing. However,
// to port it to AWS Lambda we will create a wrapper around that will load the app from
// this file
module.exports = app;
