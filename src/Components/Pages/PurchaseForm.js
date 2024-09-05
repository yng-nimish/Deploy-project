import React, { useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { loadStripe } from "@stripe/stripe-js";
import { Link, NavLink } from "react-router-dom";
import { FiArrowLeft } from "react-icons/fi";
const stripePromise = loadStripe(
  "REDACTED"
);

const PurchaseForm = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const { cartItems } = location.state || { cartItems: [] };

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
      email: "",
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

    try {
      const response = await fetch(
        "https://xobpfm5d5g.execute-api.ca-central-1.amazonaws.com/prod/checkout",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            items: cartItems,
            buyerData: {
              firstName: formData.firstName,
              lastName: formData.lastName,
              streetAddress: formData.streetAddress,
              city: formData.city,
              state: formData.state,
              zipCode: formData.zipCode,
              country: formData.country,
              email: formData.email,
            },
            ownerData: formData.sameAsOwner ? null : formData.owner,
            emailList: formData.emailList,
          }),
        }
      );

      const data = await response.json();
      console.log("API Response:", data);
      console.log("Response Status:", response.status);
      if (response.ok && data.sessionId) {
        const stripe = await stripePromise;
        const { error } = await stripe.redirectToCheckout({
          sessionId: data.sessionId,
        });

        if (error) {
          console.error("Error redirecting to Stripe Checkout:", error);
          navigate("/error");
        }
      } else {
        console.error("Invalid response or sessionId:", data);
        navigate("/error");
      }
      /*
      if (response.ok && data.url) {
        window.location.href = data.url; // forwarding customer to stripe. now
      } else {
        console.error("Error redirecting to Stripe Checkout");
        console.error("Error response data:", data);
        console.error("Error response status:", response.status);
        navigate("/error"); // Redirect to an error page or handle error
      }
        */
    } catch (error) {
      console.error("Submit error:", error); // Debug statement
      navigate("/error"); // Redirect to an error page or handle error
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
            email: "",
          }
        : {
            firstName: prev.firstName,
            lastName: prev.lastName,
            streetAddress: prev.streetAddress,
            city: prev.city,
            state: prev.state,
            zipCode: prev.zipCode,
            country: prev.country,
            email: prev.email,
          },
    }));
  };

  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="home-banner-container">
          <div className="rowC">
            <div>
              <p>
                <h7 className="primary-heading-welcome"> Purchase!! </h7>
                <h1 className="primary-heading">
                  Fill in the details to continue Purchase!!
                </h1>
              </p>
            </div>
            <div className="contact-form">
              <form onSubmit={handleSubmit}>
                <div>
                  <h2 className="primary-heading-welcome">
                    {" "}
                    Buyer's information{" "}
                  </h2>
                </div>

                <div className="input-box">
                  <label>First Name: </label> &nbsp;
                  <input
                    type="text"
                    name="firstName"
                    className="field"
                    placeholder="Enter your First Name"
                    value={formData.firstName}
                    onChange={handleChange}
                    required
                  />
                </div>
                <div className="input-box">
                  <label>Last Name:</label> &nbsp;
                  <input
                    type="text"
                    name="lastName"
                    className="field"
                    placeholder="Enter your Last Name"
                    value={formData.lastName}
                    onChange={handleChange}
                    required
                  />
                </div>
                <div className="input-box">
                  <label>Street Address:</label> &nbsp;
                  <input
                    type="text"
                    name="streetAddress"
                    className="field"
                    placeholder="Enter your Street Address"
                    value={formData.streetAddress}
                    onChange={handleChange}
                    required
                  />
                </div>
                <div className="input-box">
                  <label>City/Town:</label> &nbsp;
                  <input
                    type="text"
                    name="city"
                    className="field"
                    placeholder="Enter your City/Town"
                    value={formData.city}
                    onChange={handleChange}
                    required
                  />
                </div>
                <div className="input-box">
                  {" "}
                  &nbsp;
                  <label>State/Province:</label> &nbsp;
                  <input
                    type="text"
                    name="state"
                    className="field"
                    placeholder="Enter your State/Province"
                    value={formData.state}
                    onChange={handleChange}
                    required
                  />
                </div>
                <div className="input-box">
                  <label>Zip Code:</label> &nbsp;
                  <input
                    type="text"
                    name="zipCode"
                    value={formData.zipCode}
                    className="field"
                    placeholder="Enter Zipcode"
                    onChange={handleChange}
                    required
                  />
                </div>
                <div className="input-box">
                  <label>Country:</label> &nbsp;
                  <input
                    type="text"
                    name="country"
                    className="field"
                    placeholder="Enter Country"
                    value={formData.country}
                    onChange={handleChange}
                    required
                  />
                </div>
                <div className="input-box">
                  <label>Email:</label> &nbsp;
                  <input
                    type="email"
                    name="email"
                    className="field"
                    placeholder="Enter your Email"
                    value={formData.email}
                    onChange={handleChange}
                    required
                  />
                </div>
                <div className="input-box">
                  <label>Subscribe to email list:</label> &nbsp;
                  <input
                    type="checkbox"
                    name="emailList"
                    checked={formData.emailList}
                    onChange={handleChange}
                  />
                </div>
                <div className="input-box">
                  <label>Buyer information same as owner: </label> &nbsp;
                  <input
                    type="checkbox"
                    name="sameAsOwner"
                    checked={formData.sameAsOwner}
                    onChange={handleCheckboxChange}
                  />
                  <br />
                  <br />
                </div>
                {!formData.sameAsOwner && (
                  <>
                    <div>
                      <h2 className="primary-heading-welcome">
                        {" "}
                        Owner's Information{" "}
                      </h2>
                    </div>

                    <div className="input-box">
                      <label>First Name:</label> &nbsp;
                      <input
                        type="text"
                        name="firstName"
                        className="field"
                        placeholder="Enter your First Name"
                        value={formData.owner.firstName}
                        onChange={handleOwnerChange}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>Last Name:</label> &nbsp;
                      <input
                        type="text"
                        name="lastName"
                        className="field"
                        placeholder="Enter your Last Name"
                        value={formData.owner.lastName}
                        onChange={handleOwnerChange}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>Street Address:</label> &nbsp;
                      <input
                        type="text"
                        name="streetAddress"
                        className="field"
                        placeholder="Enter your Street Address"
                        value={formData.owner.streetAddress}
                        onChange={handleOwnerChange}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>City/Town:</label> &nbsp;
                      <input
                        type="text"
                        name="city"
                        className="field"
                        placeholder="Enter your City/Town"
                        value={formData.owner.city}
                        onChange={handleOwnerChange}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>State/Province:</label> &nbsp;
                      <input
                        type="text"
                        name="state"
                        className="field"
                        placeholder="Enter your State/Province"
                        value={formData.owner.state}
                        onChange={handleOwnerChange}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>Zip Code:</label> &nbsp;
                      <input
                        type="text"
                        name="zipCode"
                        className="field"
                        placeholder="Enter Zipcode"
                        value={formData.owner.zipCode}
                        onChange={handleOwnerChange}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>Country:</label> &nbsp;
                      <input
                        type="text"
                        name="country"
                        className="field"
                        placeholder="Enter Country"
                        value={formData.owner.country}
                        onChange={handleOwnerChange}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>Email:</label> &nbsp;
                      <input
                        type="email"
                        name="email"
                        className="field"
                        placeholder="Enter your Email"
                        value={formData.owner.email}
                        onChange={handleOwnerChange}
                        required
                      />
                    </div>
                  </>
                )}
                <button type="submit">Continue to Checkout...</button>
                <Link to="/purchase">
                  <button color="primary" href="#">
                    <FiArrowLeft />
                    &nbsp;&nbsp;&nbsp; Go Back &nbsp;&nbsp;&nbsp;
                  </button>
                </Link>
              </form>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default PurchaseForm;
