import React, { useState, useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { loadStripe } from "@stripe/stripe-js";
import { Link, NavLink } from "react-router-dom";
import { FiArrowLeft } from "react-icons/fi";
import { productsArraySun } from "./ProductsArraySun";
import Terms from "./Terms";

// Load Stripe with the environment variable

// Stripe LIVE mode
const stripePromise = loadStripe(
  process.env.REACT_APP_STRIPE_PUBLISHABLE_KEY_LIVE
);
console.log("Stripe Key:", process.env.REACT_APP_STRIPE_PUBLISHABLE_KEY_LIVE);

/*
// Stripe Test mode
const stripePromise = loadStripe(
  process.env.REACT_APP_STRIPE_PUBLISHABLE_KEY_TEST
);
console.log("Stripe Key:", process.env.REACT_APP_STRIPE_PUBLISHABLE_KEY_TEST);
*/

const PurchaseForm = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const { cartItems } = location.state || { cartItems: [] };
  const [errorMessage, setErrorMessage] = useState(null);

  const [formData, setFormData] = useState({
    firstName: "",
    lastName: "",
    business: "",
    streetAddress: "",
    city: "",
    state: "",
    zipCode: "",
    country: "",
    email: "",
    emailList: false,
    termsAgreed: false,
    owners: [],
  });

  const [validationErrors, setValidationErrors] = useState({
    buyer: { firstName: null, lastName: null },
    owners: [],
  });

  const calculateSunProductQuantity = () => {
    return cartItems.reduce((count, item) => {
      const product = productsArraySun.find(
        (product) => product.id === item.id
      );
      return product ? count + item.quantity : count;
    }, 0);
  };

  const numSunProducts = calculateSunProductQuantity();

  useEffect(() => {
    setFormData((prev) => ({
      ...prev,
      owners: Array.from({ length: numSunProducts }, () => ({
        firstName: "",
        lastName: "",
        business: "",
        streetAddress: "",
        city: "",
        state: "",
        zipCode: "",
        country: "",
        email: "",
      })),
    }));
    setValidationErrors((prev) => ({
      ...prev,
      owners: Array.from({ length: numSunProducts }, () => ({
        firstName: null,
        lastName: null,
      })),
    }));
  }, [numSunProducts]);

  const validateName = (name, fieldName) => {
    console.log(`Validating ${fieldName}: ${name}`);
    if (!name) {
      console.warn(`${fieldName} is empty`);
      return "This field is required";
    }
    if (!/^[A-Za-z]+$/.test(name)) {
      console.warn(`${fieldName} contains invalid characters: ${name}`);
      return "Only alphabetic characters allowed";
    }
    console.log(`${fieldName} is valid: ${name}`);
    return null;
  };

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: type === "checkbox" ? checked : value,
    }));

    if (name === "firstName" || name === "lastName") {
      const error = validateName(value, name);
      setValidationErrors((prev) => ({
        ...prev,
        buyer: { ...prev.buyer, [name]: error },
      }));
    }
  };

  const handleOwnerChange = (index, e) => {
    const { name, value } = e.target;
    const updatedOwners = [...formData.owners];
    updatedOwners[index] = {
      ...updatedOwners[index],
      [name]: value,
    };
    setFormData((prev) => ({
      ...prev,
      owners: updatedOwners,
    }));

    if (name === "firstName" || name === "lastName") {
      const error = validateName(value, `Owner ${index + 1} ${name}`);
      const updatedOwnerErrors = [...validationErrors.owners];
      updatedOwnerErrors[index] = {
        ...updatedOwnerErrors[index],
        [name]: error,
      };
      setValidationErrors((prev) => ({
        ...prev,
        owners: updatedOwnerErrors,
      }));
    }
  };

  const isFormValid = () => {
    const buyerValid =
      !validationErrors.buyer.firstName && !validationErrors.buyer.lastName;
    const ownersValid = validationErrors.owners.every(
      (owner) => !owner.firstName && !owner.lastName
    );
    const termsAgreed = formData.termsAgreed;
    console.log(
      `Form validation - Buyer valid: ${buyerValid}, Owners valid: ${ownersValid}, Terms agreed: ${termsAgreed}`
    );
    return buyerValid && ownersValid && termsAgreed;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    console.log("Form Data:", formData);
    console.log("Cart Items:", cartItems);
    console.log("Validation Errors:", validationErrors);

    if (!isFormValid()) {
      console.error("Form submission blocked due to validation errors");
      setErrorMessage(
        "Please correct all form errors and agree to the terms before proceeding."
      );
      return;
    }

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
              business: formData.business,
              streetAddress: formData.streetAddress,
              city: formData.city,
              state: formData.state,
              zipCode: formData.zipCode,
              country: formData.country,
              email: formData.email,
            },
            ownerData: formData.owners,
            emailList: formData.emailList,
            purchaseDate: new Date().toISOString().split("T")[0],
            country: formData.country,
          }),
        }
      );

      const data = await response.json();
      console.log("API Response:", data);
      console.log("Response Status:", response.status);

      if (!response.ok) {
        console.error("API error:", data);
        setErrorMessage(
          data.error ||
            "An error occurred during checkout. Please try again or contact support."
        );
        return;
      }

      const sessionId = data.sessionId;

      if (sessionId) {
        const stripe = await stripePromise;
        const { error } = await stripe.redirectToCheckout({
          sessionId,
        });

        if (error) {
          console.error("Stripe redirect error:", error);
          setErrorMessage(
            "Failed to redirect to payment page: " + error.message
          );
          navigate("/error");
        }
      } else {
        console.error("Invalid response or sessionId:", data);
        setErrorMessage(
          "Invalid response from server. Please try again or contact support."
        );
        navigate("/error");
      }
    } catch (error) {
      console.error("Submit error:", error);
      setErrorMessage(
        "An unexpected error occurred. Please try again or contact support."
      );
      navigate("/error");
    }
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
                  Fill in the details to continue Purchase!! and become a member
                  of Your Number Guaranteed.<br></br>
                  Please find and fill in the details of the owner, the owner is
                  the only person who will have access to our suite of free
                  applications. Once purchased, the SUN is not transferrable.
                </h1>
                {errorMessage && (
                  <p
                    style={{
                      color: "red",
                      fontSize: "1rem",
                      marginTop: "1rem",
                    }}
                  >
                    {errorMessage}
                  </p>
                )}
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
                  <label>First Name: </label>
                  <input
                    type="text"
                    name="firstName"
                    className="field"
                    placeholder="Enter your First Name"
                    value={formData.firstName}
                    onChange={handleChange}
                    required
                  />
                  {validationErrors.buyer.firstName && (
                    <p style={{ color: "red", fontSize: "0.8rem" }}>
                      {validationErrors.buyer.firstName}
                    </p>
                  )}
                </div>
                <div className="input-box">
                  <label>Last Name:</label>
                  <input
                    type="text"
                    name="lastName"
                    className="field"
                    placeholder="Enter your Last Name"
                    value={formData.lastName}
                    onChange={handleChange}
                    required
                  />
                  {validationErrors.buyer.lastName && (
                    <p style={{ color: "red", fontSize: "0.8rem" }}>
                      {validationErrors.buyer.lastName}
                    </p>
                  )}
                </div>
                <div className="input-box">
                  <label>Business/organization </label>
                  <input
                    type="text"
                    name="business"
                    className="field"
                    placeholder="Business/Organization Name"
                    value={formData.business}
                    onChange={handleChange}
                  />
                </div>
                <div className="input-box">
                  <label>Street Address:</label>
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
                  <label>City/Town:</label>
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
                  <label>State/Province:</label>
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
                  <label>Zip Code:</label>
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
                  <label>Country:</label>
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
                  <label>Email:</label>
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
                  <label>Subscribe to email list:</label>
                  <input
                    type="checkbox"
                    name="emailList"
                    checked={formData.emailList}
                    onChange={handleChange}
                  />
                </div>
                <div className="input-box">
                  <p>
                    <NavLink to="/terms">Terms </NavLink>
                  </p>
                  <label>I Agree to the Terms:</label>
                  <input
                    type="checkbox"
                    name="termsAgreed"
                    checked={formData.termsAgreed}
                    onChange={handleChange}
                    required
                  />
                </div>
                {formData.owners.map((owner, index) => (
                  <div key={index}>
                    <h2 className="primary-heading-welcome">
                      {" "}
                      Owner {index + 1}{" "}
                    </h2>
                    <label>
                      Please find and fill in the details of the owner, the
                      owner is the only person who will have access to our suite
                      of free applications. Once purchased, the SUN is not
                      transferrable.
                    </label>
                    <div className="input-box">
                      <label>First Name:</label>
                      <input
                        type="text"
                        name="firstName"
                        className="field"
                        placeholder="Enter Owner's First Name"
                        value={owner.firstName}
                        onChange={(e) => handleOwnerChange(index, e)}
                        required
                      />
                      {validationErrors.owners[index]?.firstName && (
                        <p style={{ color: "red", fontSize: "0.8rem" }}>
                          {validationErrors.owners[index].firstName}
                        </p>
                      )}
                    </div>
                    <div className="input-box">
                      <label>Last Name:</label>
                      <input
                        type="text"
                        name="lastName"
                        className="field"
                        placeholder="Enter Owner's Last Name"
                        value={owner.lastName}
                        onChange={(e) => handleOwnerChange(index, e)}
                        required
                      />
                      {validationErrors.owners[index]?.lastName && (
                        <p style={{ color: "red", fontSize: "0.8rem" }}>
                          {validationErrors.owners[index].lastName}
                        </p>
                      )}
                    </div>
                    <div className="input-box">
                      <label>Business/Organization:</label>
                      <input
                        type="text"
                        name="business"
                        className="field"
                        placeholder="Business/Organization Name"
                        value={owner.business}
                        onChange={(e) => handleOwnerChange(index, e)}
                      />
                    </div>
                    <div className="input-box">
                      <label>Street Address:</label>
                      <input
                        type="text"
                        name="streetAddress"
                        className="field"
                        placeholder="Enter your Street Address"
                        value={owner.streetAddress}
                        onChange={(e) => handleOwnerChange(index, e)}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>City/Town:</label>
                      <input
                        type="text"
                        name="city"
                        className="field"
                        placeholder="Enter your City/Town"
                        value={owner.city}
                        onChange={(e) => handleOwnerChange(index, e)}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>State/Province:</label>
                      <input
                        type="text"
                        name="state"
                        className="field"
                        placeholder="Enter your State/Province"
                        value={owner.state}
                        onChange={(e) => handleOwnerChange(index, e)}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>Zip Code:</label>
                      <input
                        type="text"
                        name="zipCode"
                        className="field"
                        placeholder="Enter Zipcode"
                        value={owner.zipCode}
                        onChange={(e) => handleOwnerChange(index, e)}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>Country:</label>
                      <input
                        type="text"
                        name="country"
                        className="field"
                        placeholder="Enter Country"
                        value={owner.country}
                        onChange={(e) => handleOwnerChange(index, e)}
                        required
                      />
                    </div>
                    <div className="input-box">
                      <label>Email:</label>
                      <input
                        type="email"
                        name="email"
                        className="field"
                        placeholder="Enter your Email"
                        value={owner.email}
                        onChange={(e) => handleOwnerChange(index, e)}
                        required
                      />
                      <br />
                    </div>
                    <br />
                  </div>
                ))}
                <button
                  style={{
                    backgroundColor: "#FFD700",
                    borderColor: "#FFD700",
                    color: "black",
                    fontSize: "1.5rem",
                    opacity: isFormValid() ? 1 : 0.5,
                    cursor: isFormValid() ? "pointer" : "not-allowed",
                  }}
                  type="submit"
                  disabled={!isFormValid()}
                >
                  Proceed to Checkout...
                </button>
                <Link to="/purchase">
                  <button color="primary" href="#">
                    <FiArrowLeft /> Go Back
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
