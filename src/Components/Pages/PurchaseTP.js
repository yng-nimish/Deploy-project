import React, { useState, useEffect } from "react";
import { Button } from "react-bootstrap";
import Swal from "sweetalert2";
import { productsArray } from "./ProductsStore"; // Ensure this import is correct

const PurchaseTP = () => {
  const [userData, setUserData] = useState({
    firstName: "",
    lastName: "",
    items: [], // Initialize as an empty array
    serialKey: "", // Add serialKey to state
    priceId: "", // Add priceId to state
  });

  const [products, setProducts] = useState([]);

  useEffect(() => {
    // Initialize products state
    if (productsArray && productsArray.length) {
      setProducts(productsArray);
    } else {
      console.error("Products data is not available.");
    }

    // Load user data and items from URL on page load
    window.onload = () => {
      const urlParams = new URLSearchParams(window.location.search);
      const firstName = urlParams.get("first_name");
      const lastName = urlParams.get("last_name");
      const itemsParam = urlParams.get("items");
      const serialKey = urlParams.get("serial_key"); // Get serial key from URL
      const priceId = urlParams.get("price_id"); // Get price_id from URL

      // Debug logs
      console.log("URL Parameters:");
      console.log("Serial Key:", serialKey);
      console.log("Price ID:", priceId);

      // Default items to an empty array if undefined
      let items = [];
      try {
        items = JSON.parse(decodeURIComponent(itemsParam)) || [];
      } catch (e) {
        console.error("Failed to parse items from URL:", e);
      }

      // Set user data
      setUserData({
        firstName: firstName || "",
        lastName: lastName || "",
        items: items,
        serialKey: serialKey || "",
        priceId: priceId || "",
      });
    };
  }, []);

  // Handle download with user feedback
  const handleDownload = (pdfUrl, title) => {
    Swal.fire({
      title: "Download",
      text: `Downloading ${title}`,
      icon: "success",
    }).then(() => {
      window.open(pdfUrl, "_blank");
    });
  };

  // Return loading message if products is not available
  if (!products.length) {
    return <div>Loading products...</div>;
  }
  // Conditional rendering based on priceId and serialKey
  const shouldShowSerialKey =
    userData.priceId === "price_1PxoiI013t2ai8cxpSKPhDJl" &&
    userData.serialKey &&
    userData.serialKey.trim() !== "" &&
    userData.serialKey !== null;

  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="Purchase-section-2">
          <div className="Purchase-Container">
            <h1 className="primary-heading">Thank You for Your Purchase!</h1>
            <h2 className="primary-heading">
              Welcome, {userData.firstName} {userData.lastName}!{" "}
            </h2>
            {shouldShowSerialKey && (
              <div className="serial-key">
                <h2>Your Serial Key:</h2>
                <pre>{userData.serialKey}</pre>{" "}
                {/* Preserve formatting with <pre> */}
              </div>
            )}
            <div className="table">
              <table>
                <thead>
                  <tr>
                    <th>Title</th>
                    <th>Download</th>
                  </tr>
                </thead>
                <tbody>
                  {products.map((item) => (
                    <tr key={item.id}>
                      <td>{item.title}</td>
                      <td>
                        <Button
                          variant="success"
                          onClick={() =>
                            handleDownload(item.pdfUrl, item.title)
                          }
                          disabled={
                            !userData.items.some(
                              (purchasedItem) => purchasedItem.id === item.id
                            )
                          }
                        >
                          Download
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default PurchaseTP;
