// stripe secret key sk_test_51PYCXa013t2ai8cxhZPVeKHWpcHRqSvQvfpaIU0FzcOTbNbPfQF0DADtcSm6lHlAVFDSqc0EBS8sS3wHPE4hNuME00QHI48Bcu

const express = require("express");
var cors = require("cors");
const stripe = require("stripe")(
  "sk_test_51PYCXa013t2ai8cxhZPVeKHWpcHRqSvQvfpaIU0FzcOTbNbPfQF0DADtcSm6lHlAVFDSqc0EBS8sS3wHPE4hNuME00QHI48Bcu"
);

const app = express();
app.use(cors({
  origin: 'https://yournumberguaranteed.com',
}));
app.use(express.static("public"));
app.use(express.json());

app.post("/checkout", async (req, res) => {
  console.log(req.body);
  const items = req.body.items;

  let lineItems = [];
  items.forEach((item) => {
    lineItems.push({
      price: item.id,
      quantity: item.quantity,
    });
  });

  const session = await stripe.checkout.sessions.create({
    line_items: lineItems,
    mode: "payment",
    success_url: "http://yournumberguaranteed.com/purchase",
    cancel_url: "http://yournumberguaranteed.com/career",
  });

  res.send(
    JSON.stringify({
      url: session.url,
    })
  );
});

app.listen(4000, () => console.log("Listening on port 4000"));
