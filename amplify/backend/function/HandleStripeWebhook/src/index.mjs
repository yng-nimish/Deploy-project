const AWS = require("aws-sdk");
const stripe = require("stripe")(
  "sk_test_51PYCXa013t2ai8cxhZPVeKHWpcHRqSvQvfpaIU0FzcOTbNbPfQF0DADtcSm6lHlAVFDSqc0EBS8sS3wHPE4hNuME00QHI48Bcu"
);
const dynamoDb = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
  const sig =
    event.headers[
      "pk_test_51PYCXa013t2ai8cxvKCOTJ6mbQ87pUgdtruBKEyM1uwvStTOBKbkMt1cbMHw6QbQlWS40jpKp9fpVY1IqU030UYv00YNLjPSTi"
    ];
  const endpointSecret = "we_1PnhNI013t2ai8cxJwN7wQeZ";

  let eventData;

  try {
    eventData = stripe.webhooks.constructEvent(event.body, sig, endpointSecret);
  } catch (err) {
    return {
      statusCode: 400,
      body: `Webhook Error: ${err.message}`,
    };
  }

  if (eventData.type === "checkout.session.completed") {
    const session = eventData.data.object;
    const paymentId = session.id;

    // Update DynamoDB table
    const params = {
      TableName: "Payments",
      Key: { paymentId },
      UpdateExpression: "set #status = :status",
      ExpressionAttributeNames: {
        "#status": "status",
      },
      ExpressionAttributeValues: {
        ":status": "paid",
      },
    };

    try {
      await dynamoDb.update(params).promise();
      return {
        statusCode: 200,
        body: JSON.stringify({ received: true }),
      };
    } catch (error) {
      return {
        statusCode: 500,
        body: `Error updating DynamoDB: ${error.message}`,
      };
    }
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ received: true }),
  };
};
