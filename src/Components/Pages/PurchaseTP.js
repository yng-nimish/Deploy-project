import React, { useState } from "react";

const PurchaseForm = () => {
  const [formData, setFormData] = useState({
    firstName: "",
    lastName: "",
    streetAddress: "",
    city: "",
    state: "",
    zipCode: "",
    country: "",
    email: "",
    emailList: false,
    sameAsOwner: false,
    owner: {
      firstName: "",
      lastName: "",
      streetAddress: "",
      city: "",
      state: "",
      zipCode: "",
      country: "",
    },
  });

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: type === "checkbox" ? checked : value,
    }));
  };

  const handleOwnerChange = (e) => {
    const { name, value } = e.target;
    setFormData((prev) => ({
      ...prev,
      owner: {
        ...prev.owner,
        [name]: value,
      },
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    // Send data to API
    const response = await fetch("https://your-api-endpoint/checkout", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(formData),
    });

    const data = await response.json();

    if (data.sessionId) {
      window.location.href = `https://checkout.stripe.com/pay/${data.sessionId}`;
    }
  };

  const handleCheckboxChange = () => {
    setFormData((prev) => ({
      ...prev,
      sameAsOwner: !prev.sameAsOwner,
      owner: prev.sameAsOwner
        ? {
            firstName: "",
            lastName: "",
            streetAddress: "",
            city: "",
            state: "",
            zipCode: "",
            country: "",
          }
        : {
            firstName: prev.firstName,
            lastName: prev.lastName,
            streetAddress: prev.streetAddress,
            city: prev.city,
            state: prev.state,
            zipCode: prev.zipCode,
            country: prev.country,
          },
    }));
  };

  return (
    <form onSubmit={handleSubmit}>
      <h2>Buyer Information</h2>
      {/* Buyer Information Fields */}
      <label>First Name:</label>
      <input
        type="text"
        name="firstName"
        value={formData.firstName}
        onChange={handleChange}
        required
      />

      <label>Last Name:</label>
      <input
        type="text"
        name="lastName"
        value={formData.lastName}
        onChange={handleChange}
        required
      />

      {/* Other Fields Similar to the Above */}

      <label>Email:</label>
      <input
        type="email"
        name="email"
        value={formData.email}
        onChange={handleChange}
        required
      />

      <label>Subscribe to email list:</label>
      <input
        type="checkbox"
        name="emailList"
        checked={formData.emailList}
        onChange={handleChange}
      />

      <label>Buyer information same as owner:</label>
      <input
        type="checkbox"
        name="sameAsOwner"
        checked={formData.sameAsOwner}
        onChange={handleCheckboxChange}
      />

      {!formData.sameAsOwner && (
        <>
          <h2>Owner Information</h2>
          {/* Owner Information Fields Similar to Buyer */}
        </>
      )}

      <button type="submit">Submit</button>
    </form>
  );
};

export default PurchaseForm;
