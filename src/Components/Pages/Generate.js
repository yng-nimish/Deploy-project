/**
 * E - commerce Server - Website code
 * We have Live mode and test mode for stripe here.
 * This is a copy of the complete code used for our Lambda Function.
 */

import serverless from "serverless-http";
import express from "express";
import cors from "cors";
import Stripe from "stripe";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
  GetCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { SESClient, SendEmailCommand } from "@aws-sdk/client-ses";

// Initialize SES client
const sesClient = new SESClient({});

// Initialize Stripe

const stripeSecretKey = process.env.STRIPE_SECRET_KEY;
const stripe = new Stripe(stripeSecretKey);

// Initialize DynamoDB client and document client
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

//Express setup
const app = express();
app.use(cors());
app.use(express.json());

const fetchPrice = async (priceId) => {
  try {
    const price = await stripe.prices.retrieve(priceId);
    if (price && price.unit_amount) {
      // Return price in dollars
      return price.unit_amount / 100;
    } else {
      console.error(`Price data is missing or invalid for ID: ${priceId}`);
      return 0;
    }
  } catch (error) {
    console.error(`Error fetching price from Stripe for ID: ${priceId}`, error);
    return 0;
  }
};

// Generate Serial Key Function
const calculateDateCode = (purchaseDateStr) => {
  const startDate = new Date(2024, 7, 24); // August is month 7 (0-based)
  const purchaseDate = new Date(purchaseDateStr);
  const dateDifference = Math.floor(
    (purchaseDate - startDate) / (1000 * 60 * 60 * 24)
  );
  const startCounter = 8240;
  return startCounter - dateDifference;
  console.log(purchaseDate);
};

const mapDateCodeToGrid = (dateCode) => {
  const dateCodeStr = String(dateCode).padStart(4, "0");
  return {
    I2: dateCodeStr[0],
    H3: dateCodeStr[1],
    E1: dateCodeStr[2],
    C1: dateCodeStr[3],
  };
};

const mapSalesCounterToGrid = (salesCounter) => {
  const salesCounterStr = String(salesCounter).padStart(9, "0");
  return {
    I1: salesCounterStr[0],
    G2: salesCounterStr[1],
    D1: salesCounterStr[2],
    B3: salesCounterStr[3],
    B1: salesCounterStr[4],
    D3: salesCounterStr[5],
    H2: salesCounterStr[6],
    A3: salesCounterStr[7],
    A1: salesCounterStr[8],
  };
};

const buyerNameCipher = {
  A: [65, 38],
  B: [10, 89],
  C: [76, 3],
  D: [80, 41],
  E: [17, 97],
  F: [81, 64],
  G: [27, 7],
  H: [45, 98],
  I: [86, 50],
  J: [1, 30],
  K: [97, 85],
  L: [40, 19],
  M: [84, 65],
  N: [23, 54],
  O: [77, 51],
  P: [55, 24],
  Q: [33, 58],
  R: [82, 63],
  S: [5, 49],
  T: [93, 36],
  U: [62, 95],
  V: [60, 20],
  W: [57, 67],
  X: [73, 56],
  Y: [9, 69],
  Z: [35, 44],
};

const ownerNameCipher = {
  A: [81, 18],
  B: [46, 53],
  C: [71, 75],
  D: [12, 37],
  E: [90, 6],
  F: [70, 87],
  G: [31, 32],
  H: [52, 74],
  I: [11, 16],
  J: [85, 47],
  K: [63, 91],
  L: [28, 66],
  M: [96, 43],
  N: [2, 8],
  O: [42, 92],
  P: [78, 59],
  Q: [21, 62],
  R: [99, 25],
  S: [51, 34],
  T: [39, 72],
  U: [28, 13],
  V: [26, 79],
  W: [83, 4],
  X: [29, 68],
  Y: [60, 48],
  Z: [14, 94],
};

const countryCodeMapping = {
  USA: 7,
  Canada: 4,
  Europe: 8,
  Asia: 9,
  "South America": 1,
  Mexico: 2,
  Africa: 5,
  China: 3,
};

const mapNameToGrid = (name, cipher, positions) => {
  const nameParts = name.toUpperCase().split(" ");
  if (nameParts.length < 2)
    throw new Error("Both first name and last name are required");

  const [firstName, lastName] = nameParts;
  const [firstInitial, lastInitial] = [firstName[0], lastName[0]];

  const [firstInitialValue1, firstInitialValue2] = cipher[firstInitial] || [
    0, 0,
  ];
  const [lastInitialValue1, lastInitialValue2] = cipher[lastInitial] || [0, 0];

  // Pad single-digit values with leading zeros
  const firstInitialDigits = String(firstInitialValue1)
    .padStart(2, "0")
    .split("")
    .map(Number);
  const lastInitialDigits = String(lastInitialValue2)
    .padStart(2, "0")
    .split("")
    .map(Number);

  return {
    [positions[0]]: firstInitialDigits[0] || 0,
    [positions[1]]: firstInitialDigits[1] || 0,
    [positions[2]]: lastInitialDigits[0] || 0,
    [positions[3]]: lastInitialDigits[1] || 0,
  };
};

// New counter for Founder series serial number
const FOUNDER_SERIES_SERIAL_NUMBER_ID = "founderSeriesSerialNumber";

// Function to fetch the founder series serial number counter
const fetchfounderSeriesSerialNumber = async () => {
  try {
    const getParams = {
      TableName: "CountersTable",
      Key: {
        FOUNDER_SERIES_SERIAL_NUMBER_ID: FOUNDER_SERIES_SERIAL_NUMBER_ID, // Correct key name
      },
    };
    console.log("Get Params:", getParams); // Debugging log

    const data = await docClient.send(new GetCommand(getParams));
    return data.Item ? data.Item.counter : 0; // Default to 0 if not found
  } catch (error) {
    console.error("Error fetching Founder series sales counter:", error);
    throw new Error("Could not fetch Founder series sales counter");
  }
};
// Function to update the specific counter
const updatefounderSeriesSerialNumber = async (newCounterValue) => {
  try {
    const updateParams = {
      TableName: "CountersTable",
      Key: {
        FOUNDER_SERIES_SERIAL_NUMBER_ID: "founderSeriesSerialNumber",
      },
      UpdateExpression: "SET #c = :newCounter",
      ExpressionAttributeNames: { "#c": "counter" },
      ExpressionAttributeValues: {
        ":newCounter": newCounterValue,
      },
    };
    console.log("Update Params:", updateParams); // Debugging log

    await docClient.send(new UpdateCommand(updateParams));
    console.log("Counter updated successfully"); // Debugging log
  } catch (error) {
    console.error("Error updating founder series sales counter:", error);
    throw new Error("Could not update founder series sales counter");
  }
};

const get4DigitNumber = (founderSeriesSerialNumber) => {
  // Use the founderSeriesSerialNumber directly
  return String(founderSeriesSerialNumber).padStart(4, "0");
  //  const offset = salesCounter - initialCounter;
  //  return String(offset).padStart(4, "0");
};
const assembleSerialNumber = (
  purchaseDate,
  salesCounter,
  buyerName,
  ownerName,
  country,
  series,
  founderSeriesSerialNumber
) => {
  const dateCode = calculateDateCode(purchaseDate);
  const dateCodeMapping = mapDateCodeToGrid(dateCode);
  const salesCounterMapping = mapSalesCounterToGrid(salesCounter);

  const buyerNameMapping = mapNameToGrid(buyerName, buyerNameCipher, [
    "H1",
    "G3",
    "G1",
    "C2",
  ]);
  const ownerNameMapping = mapNameToGrid(ownerName, ownerNameCipher, [
    "C3",
    "I3",
    "B2",
    "A2",
  ]);

  const countryCode = countryCodeMapping[country] || 6;
  const countryCodeMappingFinal = { D2: countryCode };

  const seriesLetter =
    {
      founder: "F",
      builder: "B",
      grandparent: "G",
    }[series.toLowerCase()] || "X";

  const seriesMapping = { E2: seriesLetter };

  const initialSalesCounter = 177402716;
  const incrementedNumber = get4DigitNumber(founderSeriesSerialNumber);

  const incrementedNumberMapping = {
    E3: incrementedNumber[0],
    F1: incrementedNumber[1],
    F2: incrementedNumber[2],
    F3: incrementedNumber[3],
  };

  return {
    ...dateCodeMapping,
    ...salesCounterMapping,
    ...buyerNameMapping,
    ...ownerNameMapping,
    ...countryCodeMappingFinal,
    ...incrementedNumberMapping,
    ...seriesMapping,
  };
};
const J1 = " ";

const formatGrid = (serialNumberGrid) => {
  // Define the layout for the 3x3 matrix
  const rows = [
    ["A1", "A2", "A3", "J1", "B1", "B2", "B3", "J1", "C1", "C2", "C3"],
    ["D1", "D2", "D3", "J1", "E1", "E2", "E3", "J1", "F1", "F2", "F3"],
    ["G1", "G2", "G3", "J1", "H1", "H2", "H3", "J1", "I1", "I2", "I3"],
  ];

  // Function to format each cell value
  const formatCell = (value) => {
    if (value === undefined || value === null) {
      return ""; // Handle undefined or null values gracefully
    }
    const stringValue = String(value);
    // Ensure each cell is exactly 3 characters wide, pad with spaces on the left if needed
    return stringValue;
  };

  // Function to format each block into a single line
  const formatRow = (row) => {
    return row.map((cell) => formatCell(serialNumberGrid[cell])).join(""); // Join cells with a space
  };

  // Create formatted output
  const formattedOutput = rows.map((row) => {
    // Format each row with proper spacing
    const formattedRow = formatRow(row);
    return formattedRow.trim(); // Trim any trailing spaces
  });

  return formattedOutput.join("\n");
};

const generateSerialKeys = (
  purchaseDate,
  salesCounter,
  buyerName,
  ownerNames,
  country,
  series,
  startingSerialNumber,
  quantity
) => {
  return ownerNames.map((ownerName, index) => {
    const serialNumberGrid = assembleSerialNumber(
      purchaseDate,
      salesCounter,
      buyerName,
      ownerName,
      country,
      series,
      startingSerialNumber + index
    );
    const formattedKey = formatGrid(serialNumberGrid);
    console.log("Formatted Key is \n" + formattedKey);
    return formattedKey + "\n\n";
  });
};

const sendEmail = async (toAddress, subject, body) => {
  const params = {
    Destination: {
      ToAddresses: [toAddress],
    },
    Message: {
      Body: {
        Html: {
          Charset: "UTF-8",
          Data: body,
        },
      },
      Subject: {
        Charset: "UTF-8",
        Data: subject,
      },
    },
    Source: "ceo@yournumberguaranteed.com", // Set to CEO email (earlier was set to yng.nimishs@icloud.com)
  };

  try {
    const command = new SendEmailCommand(params);
    await sesClient.send(command);
    console.log(`Email sent to ${toAddress}`);
  } catch (error) {
    console.error(`Error sending email to ${toAddress}:`, error);
  }
};

app.post("/checkout", async (req, res) => {
  console.log("Request body:", req.body);
  // Try passing the data with the request parameter.

  // Actual code for Live mode
  const {
    items,
    buyerData,
    ownerData = [], // Default to empty array if ownerData is not provided
    emailList,
    purchaseDate,
    country,
  } = req.body;

  // Test code with hardcoded Date Value
  // const { items, buyerData, ownerData = [], emailList, country } = req.body;
  // const purchaseDate = "2024-09-16"; // Hardcoded purchase date

  if (!buyerData || !buyerData.firstName || !buyerData.lastName) {
    console.error("Invalid buyerData:", buyerData);
    return res.status(400).json({ error: "Invalid buyer data" });
  }
  console.log("Owner data received:", ownerData);

  const actualOwnerData = ownerData.length > 0 ? ownerData : [buyerData]; // Use buyerData if ownerData is null or empty

  // Validate actualOwnerData
  if (
    !Array.isArray(actualOwnerData) ||
    actualOwnerData.some((owner) => !owner.firstName || !owner.lastName)
  ) {
    console.error("Invalid ownerData:", actualOwnerData);
    return res.status(400).json({ error: "Invalid owner data" });
  }

  let lineItems = [];
  let totalCost = 0;
  let togenerateSerialKey = false;
  let priceId = ""; // Initialize priceId

  console.log("Processing items:", items);
  console.log("Total Cost is " + totalCost);
  // Initialize quantityOfFounderSeriesSun
  let quantityOfFounderSeriesSun = 0;

  for (const item of items) {
    if (!item.id || !item.quantity) {
      console.error("Invalid item data:", item);
      continue; // Skip invalid items
    }

    try {
      // Fetch price from Stripe
      const productPrice = await fetchPrice(item.id);
      console.log(`Fetched Price: ${productPrice}, Quantity: ${item.quantity}`);

      if (!isNaN(productPrice) && productPrice > 0) {
        totalCost += productPrice * parseInt(item.quantity);

        // Check if the current item ID matches the Sun price ID
        //Test Mode
        // if (item.id === "price_1PxoiI013t2ai8cxpSKPhDJl") {
        // Live Mode
        if (item.id === "price_1Py2vL013t2ai8cxo0WMZZHi") {
          togenerateSerialKey = true;
          quantityOfFounderSeriesSun += parseInt(item.quantity, 10); // Increment quantity
        }
      } else {
        console.error(`Invalid price value for item ID: ${item.id}`);
      }
    } catch (error) {
      console.error(`Error processing item ID: ${item.id}`, error);
    }
  }

  // Add items to line items and calculate total cost
  items.forEach((item) => {
    lineItems.push({
      price: item.id,
      quantity: item.quantity,
    });
    console.log("item price is" + item.price);
    const productPrice = parseFloat(item.price);

    if (!isNaN(productPrice)) {
      totalCost += productPrice * parseInt(item.quantity);
      totalCost += productData.price * cartItem.quantity;
    }
    console.log(`Parsed Price: ${productPrice}, Quantity: ${item.quantity}`);
    console.log("Total Cost is " + totalCost);
  });

  // Add processing fee if total cost is less than $50
  console.log("Line Items before adding fee:", lineItems);
  console.log("Total Cost before processing fee:", totalCost);

  if (totalCost < 50) {
    console.log("Total Cost is less than $50, adding processing fee.");
    lineItems.push({
      //   price: "price_1Px8XL013t2ai8cxAOSkYTjB", // Test Mode - processing fee

      price: "price_1Py2vR013t2ai8cxsp6eOczL", // Live Mode - processing fee
      quantity: 1,
    });
  }
  console.log("Line Items after adding fee:", lineItems);

  // Generate a unique customer ID

  const customerId = `customer-${Date.now()}`;

  // Fetch the current sales counter
  let salesCounter;
  try {
    const getParams = {
      TableName: "SalesCounterTable",
      Key: { id: "salesCounter" },
    };
    const data = await docClient.send(new GetCommand(getParams));
    salesCounter = data.Item ? data.Item.counter : 177402716; // Default counter if not found
  } catch (error) {
    console.error("Error fetching sales counter:", error);
    return res.status(500).json({ error: "Could not fetch sales counter" });
  }

  // Fetch the current founder series sales counter
  let founderSeriesSerialNumber;
  try {
    founderSeriesSerialNumber = await fetchfounderSeriesSerialNumber();
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }

  let serialKeys = [];
  let finalFounderSeriesSerialNumber = founderSeriesSerialNumber;

  // Generate the serial key only if required
  if (togenerateSerialKey) {
    serialKeys = generateSerialKeys(
      purchaseDate,
      salesCounter,
      `${buyerData.firstName} ${buyerData.lastName}`,
      actualOwnerData.map((owner) => `${owner.firstName} ${owner.lastName}`),
      country,
      "founder",
      finalFounderSeriesSerialNumber,
      quantityOfFounderSeriesSun
    );
    finalFounderSeriesSerialNumber += quantityOfFounderSeriesSun;

    // Update the founder series serial number
    try {
      await updatefounderSeriesSerialNumber(finalFounderSeriesSerialNumber);
    } catch (error) {
      console.error("Error updating founder series serial number:", error);
      return res.status(500).json({ error: error.message });
    }
  }
  // Update the sales counter with a different attribute name
  try {
    const updateParams = {
      TableName: "SalesCounterTable",
      Key: { id: "salesCounter" },
      UpdateExpression: "SET #newCounter = :newCounter",
      ExpressionAttributeNames: {
        "#newCounter": "counter", // Use a different name temporarily
      },
      ExpressionAttributeValues: {
        ":newCounter": salesCounter + 1,
      },
    };
    await docClient.send(new UpdateCommand(updateParams));
  } catch (error) {
    console.error("Error updating sales counter:", error);
    return res.status(500).json({ error: "Could not update sales counter" });
  }

  const params = {
    TableName: "CustomerTable",
    Item: {
      id: customerId,
      serialKey: serialKeys, // Save serial key // convert serial key to array.
      buyerData: buyerData,
      ownerData: actualOwnerData,
      emailList: emailList,
      items: items,
    },
  };

  // Save customer data to DynamoDB
  try {
    console.log("Saving to DynamoDB:", params);
    await docClient.send(new PutCommand(params));
  } catch (error) {
    console.error("Error saving customer data:", error);
    return res.status(500).json({ error: "Could not save customer data" });
  }
  // Collect all price IDs
  const priceIds = items.map((item) => item.id).join(",");
  console.log("Price IDs:", priceIds);

  // Create Stripe Checkout session

  try {
    const session = await stripe.checkout.sessions.create({
      line_items: lineItems,
      mode: "payment",
      success_url: `https://yournumberguaranteed.com/purchaseTP?session_id={CHECKOUT_SESSION_ID}&serial_key=${encodeURIComponent(
        serialKeys.join(",")
      )}&first_name=${encodeURIComponent(
        buyerData.firstName
      )}&last_name=${encodeURIComponent(
        buyerData.lastName
      )}&items=${encodeURIComponent(
        JSON.stringify(items)
      )}&price_id=${encodeURIComponent(
        priceIds
      )}&owner_data=${encodeURIComponent(JSON.stringify(actualOwnerData))}`, // Include owner_data in the URL

      cancel_url: "https://yournumberguaranteed.com/purchase",
      metadata: {
        customerId: customerId,
        serialKeys: serialKeys.join(","),
        items: JSON.stringify(items),
      },
    });

    // Send email to the owner of the Sun series
    if (togenerateSerialKey) {
      const subject = "Thank You for Purchasing the Founder Series SUN !";
      const body = `
      <p>Dear Owner,</p>
      <p>Thank you for purchasing your SUN </p>
      <p>Please see your Serial Number Below: Your serial number is unique to you, and your SUN will be Guaranteed Unique too. 
      The serial number shows that you have purchased a Founders Series SUN, denoted by the center of the serial number featuring 
      F0 000 to F9 999. <br>
      There are only 10,000 of these available, and you are one of the small group of members that are with us from the very start. 
      There will be merchandise available to members with this series of serial numbers. <br>
      <br>
      The SUN will be available for download starting on Black Friday, November 29th. 
      At that time we will be introducing some of our first applications. 
      Those applications will be The Snippet and Research apps. 
      We are working on building out the other applications as we move forward, 
      and we will notify all members as we constantly release new applications.
      <br>    
      We thank you for making this possible, and we ask you to spread the word about our community and invite your friends and family to join us, and support us by purchasing a SUN, buying and reading our book, using our IP in partnerships with your organization.
      <br>
      Your Serial Number is:<br>${serialKeys.join(", ")}			

      <br>
    Please keep it safe, you will need it to download your SUN on November 29th, 2024.
    Also, Black Friday is the release date of our new book “Your Number Guaranteed: Birth and Infancy in the Year 2024”
    </p>
      <p>Best regards,
      <br>Your Number Guaranteed Inc.</p>
    `;

      try {
        // Send email to each owner
        await Promise.all(
          actualOwnerData.map((owner) => {
            const emailAddress = owner.email; // Assume email is part of ownerData
            return sendEmail(emailAddress, subject, body);
          })
        );
      } catch (error) {
        console.error("Error sending email:", error);
      }
    }

    res.json({ sessionId: session.id });
  } catch (error) {
    console.error("Error creating Stripe Checkout session:", error);
    res.status(500).json({ error: "Could not create checkout session" });
  }
});
export const handler = serverless(app);
