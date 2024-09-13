import React, { useState, useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { loadStripe } from "@stripe/stripe-js";
import { Link, NavLink } from "react-router-dom";
import { FiArrowLeft } from "react-icons/fi";
import { productsArraySun } from "./ProductsArraySun";
/* Test Mode
const stripePromise = loadStripe(
  "pk_test_51PYCXa013t2ai8cxvKCOTJ6mbQ87pUgdtruBKEyM1uwvStTOBKbkMt1cbMHw6QbQlWS40jpKp9fpVY1IqU030UYv00YNLjPSTi"
);
*/
// Live Mode
const stripePromise = loadStripe(
  "pk_live_51PYCXa013t2ai8cx8TMVzR5XyFKBg1or1U8kZpBudEMObvxQETCZxkiqL3JNFiGdNLeFe9NhuCz58yZto5KIO4Xr00JwUxiYsc"
);

const PurchaseForm = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const { cartItems } = location.state || { cartItems: [] };

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
    sameAsOwner: false,
    owners: [], // Array to hold owner information
  });

  {
    /*
    owners: Array.from({ length: sunProductCount }, () => ({
      firstName: "",
      lastName: "",
      business: "",
      streetAddress: "",
      city: "",
      state: "",
      zipCode: "",
      country: "",
      email: "",
    })), */
  }

  // Calculate the number of SUN products in the cart
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
    // Initialize owner fields based on the number of SUN products
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
  }, [numSunProducts]);

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: type === "checkbox" ? checked : value,
    }));
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
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    // Assume `cartItems` may contain `productsArray` and `productsArraySun`
    console.log("Cart Items:", cartItems);

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
            ownerData: formData.sameAsOwner
              ? [
                  // Have to wrap as an Array for same as owner button
                  {
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
                ]
              : formData.owners,
            emailList: formData.emailList,
            purchaseDate: new Date().toISOString().split("T")[0], // Assuming current date

            country: formData.country,
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
      owners: prev.sameAsOwner
        ? [
            {
              firstName: "",
              lastName: "",
              business: "",
              streetAddress: "",
              city: "",
              state: "",
              zipCode: "",
              country: "",
              email: "",
            },
          ]
        : prev.owners,
    }));
  };

  {
    /*
        ? Array.from({ length: sunProductCount }, () => ({
            firstName: "",
            lastName: "",
            business: "",
            streetAddress: "",
            city: "",
            state: "",
            zipCode: "",
            country: "",
            email: "",
          }))
        : prev.owners,
    }));
    };
    */
  }

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
                {/* Buyer's Information Fields */}
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
                  <div className="input-box">
                    <label>Business/organization </label> &nbsp;
                    <input
                      type="text"
                      name="business"
                      className="field"
                      placeholder="Business/Organization Name"
                      value={formData.business}
                      onChange={handleChange}
                    />
                  </div>
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
                {/* Render Owner Fields Dynamically */}
                {/* Render Owner Fields Dynamically */}
                {!formData.sameAsOwner &&
                  formData.owners.map((owner, index) => (
                    <div key={index}>
                      <h2 className="primary-heading-welcome">
                        {" "}
                        Owner {index + 1}{" "}
                      </h2>
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
                      </div>
                      <div className="input-box">
                        <label>Business/Organization:</label> &nbsp;
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
                        <label>Street Address:</label> &nbsp;
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
                        <label>City/Town:</label> &nbsp;
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
                        <label>State/Province:</label> &nbsp;
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
                        <label>Zip Code:</label> &nbsp;
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
                        <label>Country:</label> &nbsp;
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
                        <label>Email:</label> &nbsp;
                        <input
                          type="email"
                          name="email"
                          className="field"
                          placeholder="Enter your Email"
                          value={owner.email}
                          onChange={(e) => handleOwnerChange(index, e)}
                          required
                        />{" "}
                        <br />
                      </div>
                      <br />
                    </div>
                  ))}
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
