import React, { useState, useEffect } from "react";
import { Button } from "react-bootstrap";
import Swal from "sweetalert2";
import { productsArray } from "./ProductsStore"; // Adjust path as needed
import { Link } from "react-router-dom";
import { FiArrowRight } from "react-icons/fi";
import {
  CCard,
  CCardBody,
  CCardTitle,
  CCardText,
  CButton,
  CRow,
  CCol,
} from "@coreui/react";
import axios from "axios";

const PurchaseTP = () => {
  const [userData, setUserData] = useState({
    firstName: "",
    lastName: "",
    items: [],
    serialKeys: [], // Array of { serialKey, owner }
    priceIds: [],
    ownerData: [],
  });

  const [products, setProducts] = useState([]);

  useEffect(() => {
    // Initialize products
    if (productsArray && productsArray.length) {
      setProducts(productsArray);
    } else {
      console.error("Products data is not available.");
    }

    // Parse URL parameters
    const urlParams = new URLSearchParams(window.location.search);
    const firstName = urlParams.get("first_name");
    const lastName = urlParams.get("last_name");
    const itemsParam = urlParams.get("items");
    const priceIdsParam = urlParams.get("price_id");
    const ownerDataParam = urlParams.get("owner_data");
    const sessionId = urlParams.get("session_id");

    console.log("URL:", window.location.href);
    console.log("URL Parameters:", {
      firstName,
      lastName,
      itemsParam,
      priceIdsParam,
      ownerDataParam,
      sessionId,
    });

    let items = [];
    try {
      items = JSON.parse(decodeURIComponent(itemsParam)) || [];
    } catch (e) {
      console.error("Failed to parse items from URL:", e);
    }

    let ownerData = [];
    try {
      ownerData = JSON.parse(decodeURIComponent(ownerDataParam)) || [];
    } catch (e) {
      console.error("Failed to parse owner data from URL:", e);
    }

    let priceIds = [];
    if (priceIdsParam) {
      priceIds = decodeURIComponent(priceIdsParam).split(",");
    }

    // Fetch serial keys
    const fetchSerialKeys = async () => {
      if (sessionId) {
        try {
          const response = await axios.get(
            `https://xobpfm5d5g.execute-api.ca-central-1.amazonaws.com/prod/getSerialKeys/${sessionId}`
          );
          const serialKeys = response.data.serialKeys.map((key, index) => ({
            serialKey: key,
            owner: ownerData[index] || {},
          }));
          setUserData({
            firstName: firstName || "",
            lastName: lastName || "",
            items,
            serialKeys,
            priceIds,
            ownerData,
          });
        } catch (error) {
          console.error("Failed to fetch serial keys:", error);
          setUserData({
            firstName: firstName || "",
            lastName: lastName || "",
            items,
            serialKeys: [],
            priceIds,
            ownerData,
          });
        }
      } else {
        setUserData({
          firstName: firstName || "",
          lastName: lastName || "",
          items,
          serialKeys: [],
          priceIds,
          ownerData,
        });
      }
    };

    fetchSerialKeys();
  }, []);

  const handleDownload = (pdfUrl, title) => {
    Swal.fire({
      title: "Download",
      text: `Downloading ${title}`,
      icon: "success",
    }).then(() => {
      window.open(pdfUrl, "_blank");
    });
  };

  if (!products.length) {
    return <div>Loading products...</div>;
  }

  const shouldShowSerialKey =
    userData.priceIds.includes("price_1PxoiI013t2ai8cxpSKPhDJl") &&
    userData.serialKeys &&
    userData.serialKeys.length > 0;

  const formatGrid = (serialKey) => {
    const rows = [
      ["A1", "A2", "A3", "J1", "B1", "B2", "B3", "J1", "C1", "C2", "C3"],
      ["D1", "D2", "D3", "J1", "E1", "E2", "E3", "J1", "F1", "F2", "F3"],
      ["G1", "G2", "G3", "J1", "H1", "H2", "H3", "J1", "I1", "I2", "I3"],
    ];
    const grid = {};
    let keyIndex = 0;
    rows.flat().forEach((cell) => {
      grid[cell] = serialKey[keyIndex] || "";
      keyIndex++;
    });

    const formatCell = (value) => String(value || "");
    const formatRow = (row) =>
      row.map((cell) => formatCell(grid[cell])).join("");
    return rows.map((row) => formatRow(row).trim()).join("\n");
  };

  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="Purchase-section-2">
          <div className="Purchase-Container">
            <h1 className="primary-heading">Thank You for Your Purchase!</h1>
            <h2 className="primary-heading">
              Welcome, {userData.firstName} {userData.lastName}!
            </h2>
            {shouldShowSerialKey && (
              <div className="serial-keys">
                <h2>
                  Your Serial Key: Save it, you will need it to download The SUN
                </h2>
                {userData.serialKeys.map((key, index) => (
                  <div key={index} className="serial-key">
                    <h3>Serial Key {index + 1}:</h3>
                    <pre
                      style={{
                        whiteSpace: "pre-wrap",
                        fontFamily: "Courier New, monospace",
                      }}
                    >
                      {formatGrid(key.serialKey)}
                    </pre>
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
              <h1 className="primary-heading-2">Download The SUN</h1>
              <CCard color="white" className="mb-3">
                <CRow className="g-0">
                  <CCol md={8}>
                    <CCardBody>
                      <CCardTitle>Download</CCardTitle>
                      <CCardText>Download your SUN</CCardText>
                    </CCardBody>
                  </CCol>
                  <CCol md={4} className="mb-3 pl-3 my-auto mx-auto col-6">
                    <Link to="/verify">
                      <CButton color="primary">
                        Download <FiArrowRight />
                      </CButton>
                    </Link>
                  </CCol>
                </CRow>
              </CCard>
            </div>
            <div className="table">
              <h1 className="primary-heading-2">
                Download The Technical Papers
              </h1>
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
                          type="primary"
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
              <CCardBody>
                <CCardTitle>Verify</CCardTitle>
                <CCardText>Verify your Serial Key</CCardText>
              </CCardBody>
            </CCol>
            <CCol md={4} className="mb-3 pl-3 my-auto mx-auto col-6">
              <Link to="/verify">
                <CButton color="primary">
                  Verify <FiArrowRight />
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
