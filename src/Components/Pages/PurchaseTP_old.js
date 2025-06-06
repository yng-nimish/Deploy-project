/**
 * E - commerce PurchaseTP Page - Website code - Used as landing page post payment to be able to download Technical Papers.
 * We have Live mode and test mode for stripe here.
 */

import React, { useState, useEffect } from "react";
import { Button } from "react-bootstrap";
import Swal from "sweetalert2";
import { productsArray } from "./ProductsStore"; // Ensure this import is correct
import { Link, NavLink } from "react-router-dom";
import { FiArrowRight } from "react-icons/fi";
import { Row, Col } from "react-bootstrap";
import { productsArraySun } from "./ProductsArraySun";
import ProductCard from "./ProductCard";

import { CCard } from "@coreui/react";
import { CCardImage } from "@coreui/react";
import { CCardBody } from "@coreui/react";
import { CCardTitle } from "@coreui/react";
import { CCardText } from "@coreui/react";
import { CButton } from "@coreui/react";
import { CRow } from "@coreui/react";
import { CCol } from "@coreui/react";
const PurchaseTP = () => {
  const [userData, setUserData] = useState({
    firstName: "",
    lastName: "",
    items: [], // Initialize as an empty array ok
    serialKeys: [], // Update to hold an array of serial key objects
    priceIds: [], // Add priceIds to state
    ownerData: [], //get owner data from
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
    const handleLoad = () => {
      const urlParams = new URLSearchParams(window.location.search);
      const firstName = urlParams.get("first_name");
      const lastName = urlParams.get("last_name");
      const itemsParam = urlParams.get("items");
      const serialKeysParam = urlParams.get("serial_key"); // Corrected parameter name
      const priceIdsParam = urlParams.get("price_id"); // Corrected parameter name
      const ownerDataParam = urlParams.get("owner_data"); // New parameter for owner data

      // Debug logs
      console.log("URL:", window.location.href);
      console.log("URL Parameters:");
      console.log("First Name:", firstName);
      console.log("Last Name:", lastName);
      console.log("Items Param:", itemsParam);
      console.log("Serial Keys:", serialKeysParam); // Corrected parameter name
      console.log("Price IDs:", priceIdsParam);
      console.log("Owner Data:", ownerDataParam); // New debug log for owner data

      //

      // Default items to an empty array if undefined
      let items = [];
      try {
        items = JSON.parse(decodeURIComponent(itemsParam)) || [];
      } catch (e) {
        console.error("Failed to parse items from URL:", e);
      }

      let serialKeys = [];
      try {
        serialKeys = decodeURIComponent(serialKeysParam)
          .split("\n\n,")
          .filter(Boolean) // Remove empty strings
          .map((key) => key.trim())
          .filter(Boolean); // Remove any leftover empty strings
      } catch (e) {
        console.error("Failed to parse serial keys from URL:", e);
      }

      // Determine the quantity of the specific priceId
      //Test Mode
      const specificPriceId = "price_1PxoiI013t2ai8cxpSKPhDJl";
      //Live Mode
      //const specificPriceId = "price_1Py2vL013t2ai8cxo0WMZZHi";

      const priceIdItem = items.find((item) => item.id === specificPriceId);
      const quantity = priceIdItem ? priceIdItem.quantity : 0;

      // Map serial keys to objects with owners based on the quantity
      serialKeys = serialKeys
        .slice(0, quantity)
        .map((key) => ({ serialKey: key, owner: {} }));

      // Parse owner data if available
      let ownerData = [];
      if (ownerDataParam) {
        try {
          ownerData = JSON.parse(decodeURIComponent(ownerDataParam)) || [];
        } catch (e) {
          console.error("Failed to parse owner data from URL:", e);
        }

        // Assign owners to serial keys
        serialKeys = serialKeys.map((key, index) => ({
          ...key,
          owner: ownerData[index] || {},
        }));
      }

      // Default priceIds to an empty array if undefined
      let priceIds = [];
      if (priceIdsParam) {
        priceIds = decodeURIComponent(priceIdsParam).split(","); // Split price_ids into an array
      }

      // Set user data
      setUserData({
        firstName: firstName || "",
        lastName: lastName || "",
        items: items,
        serialKeys: serialKeys,
        priceIds: priceIds,
      });
    };
    handleLoad();
  }, []);
  console.log("Serial Keys are:", userData.serialKeys);

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
    //Test Mode
    userData.priceIds.includes("price_1PxoiI013t2ai8cxpSKPhDJl") &&
    // Live Mode
    //userData.priceIds.includes("price_1Py2vL013t2ai8cxo0WMZZHi") &&
    userData.serialKeys &&
    userData.serialKeys.length > 0 &&
    userData.serialKeys !== null;

  const J1 = " ";

  const formatGrid = (serialKey) => {
    // Define the layout for the 3x3 matrix
    const rows = [
      ["A1", "A2", "A3", "J1", "B1", "B2", "B3", "J1", "C1", "C2", "C3"],
      ["D1", "D2", "D3", "J1", "E1", "E2", "E3", "J1", "F1", "F2", "F3"],
      ["G1", "G2", "G3", "J1", "H1", "H2", "H3", "J1", "I1", "I2", "I3"],
    ];
    // Create an object to map serial key values to the grid positions
    const grid = rows.flat().reduce((acc, cell) => {
      acc[cell] = "";
      return acc;
    }, {});

    // Fill the grid with serial key values
    let keyIndex = 0;
    for (const cell of Object.keys(grid)) {
      if (keyIndex < serialKey.length) {
        grid[cell] = serialKey[keyIndex];
        keyIndex += 1;
      }
    }

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
      return row.map((cell) => formatCell(serialKey[cell])).join(""); // Join cells with a space
    };

    // Create formatted output
    const formattedOutput = rows.map((row) => {
      // Format each row with proper spacing
      const formattedRow = formatRow(row);
      return formattedRow.trim(); // Trim any trailing spaces
    });
    console.log("Formatting Serial Key:\n" + serialKey);
    const finalOutput = formattedOutput.join("\n");

    // Replace newlines with <br> for HTML rendering
    return finalOutput;
  };

  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="Purchase-section-2">
          <div className="Purchase-Container">
            <h1 className="primary-heading">Thank You for Your Purchase!</h1>
            <h2 className="primary-heading">
              Welcome, {userData.firstName} {userData.lastName}!{" "}
            </h2>
            {/* Display Serial Keys and Owners */}

            {shouldShowSerialKey && (
              <div className="serial-keys">
                <h2>
                  Your Serial Key: Your Serial Key is Important, Save it , you
                  will need it to download The SUN
                </h2>
                {/* Map through the serial keys to display them */}

                {userData.serialKeys.map((key, index) => (
                  <div key={index} className="serial-key">
                    <h2>Serial Key {index + 1}:</h2>
                    <div>
                      <div>
                        <pre
                          style={{
                            whiteSpace: "pre-wrap",
                            fontFamily: "Courier New, monospace",
                          }}
                        >
                          {console.log(
                            "Formatting using p tag serialKey\n" + key.serialKey
                          )}
                          {key.serialKey || "Generating Serial Key"}
                        </pre>
                      </div>
                    </div>
                    {/* Add owner details here if available */}
                    <h3>Owner Details:</h3>
                    <p>
                      Name: {key.owner?.firstName || "N/A"}{" "}
                      {key.owner?.lastName || "N/A"}
                    </p>
                    <p>Email: {key.owner?.email || "N/A"}</p>
                  </div>
                ))}
              </div>
            )}

            <div>
              <h1 className="primary-heading-2">
                {" "}
                <br />
                Download The SUN <br />
              </h1>
              <div>
                <CCard color="white" className="mb-3">
                  <CRow className="g-0">
                    <CCol md={8}>
                      <CCardBody className="pre-rectangle">
                        <CCardTitle>Download</CCardTitle>
                        <CCardText>Download your SUN</CCardText>
                      </CCardBody>
                    </CCol>
                    <CCol className="mb-3 pl-3 my-auto mx-auto col-6" md={4}>
                      <Link to="/verify">
                        <CButton color="primary" href="#">
                          Download &nbsp;&nbsp;&nbsp; <FiArrowRight />
                        </CButton>
                      </Link>
                    </CCol>
                  </CRow>
                </CCard>
              </div>

              <div className="table"></div>
            </div>
            <div className="table">
              <div>
                <h1 className="primary-heading-2">
                  {" "}
                  <br />
                  Download The Technical Papers <br />
                </h1>
              </div>
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
        <CCard color="white" className="mb-3">
          <CRow className="g-0">
            <CCol md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>Verify</CCardTitle>
                <CCardText>Verify your Serial Key</CCardText>
              </CCardBody>
            </CCol>
            <CCol className="mb-3 pl-3 my-auto mx-auto col-6" md={4}>
              <Link to="/verify">
                <CButton color="primary" href="#">
                  Verify &nbsp;&nbsp;&nbsp; <FiArrowRight />
                </CButton>
              </Link>
            </CCol>
          </CRow>
        </CCard>
      </div>
    </div>
  );
};

export default PurchaseTP;
