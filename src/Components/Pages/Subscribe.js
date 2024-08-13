import { loadStripe } from "@stripe/stripe-js";

import React from "react";

export default function Subscribe() {
  const handleClick = async (e) => {
    const stripe = await loadStripe(
      "pk_test_51PYCXa013t2ai8cxvKCOTJ6mbQ87pUgdtruBKEyM1uwvStTOBKbkMt1cbMHw6QbQlWS40jpKp9fpVY1IqU030UYv00YNLjPSTi"
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
      cancelUrl: "https://www.yournumberguaranteed.com/login",
    });
  };
  return (
    <div>
      <button onClick={handleClick}>Download paper</button>
    </div>
  );
}
