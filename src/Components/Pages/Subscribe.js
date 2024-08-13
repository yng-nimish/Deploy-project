import { loadStripe } from "@stripe/stripe-js";

import React from "react";

export default function Subscribe() {
  const handleClick = async (e) => {
    const stripe = await loadStripe(
      "REDACTED"
    );
    const { error } = await stripe.redirectToCheckout({
      lineItems: [
        {
          price: "price_1Pn40F013t2ai8cxwVWKX4So",
          quantity: 1,
        },
      ],
      mode: "payment",
      successUrl: "https://yournumberguaranteed.com/career",
      cancelUrl: "https://localhost:3000/cancel",
    });
  };
  return (
    <div>
      <button onClick={handleClick}>Download paper</button>
    </div>
  );
}
