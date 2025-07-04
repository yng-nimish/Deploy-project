import React, { useState, useEffect } from "react";
import { Button, Modal } from "react-bootstrap";
import Swal from "sweetalert2";
import { productsArray } from "./ProductsStore"; // Ensure this import is correct
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
    serialKeys: [],
    priceIds: [],
  });

  const [products, setProducts] = useState([]);
  const [loadingKeys, setLoadingKeys] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (productsArray && Array.isArray(productsArray)) {
      setProducts(productsArray);
    } else {
      console.error("No products data available");
    }

    const urlParams = new URLSearchParams(window.location.search);
    const firstName = urlParams.get("first_name");
    const lastName = urlParams.get("last_name");
    const priceIdsParam = urlParams.get("price_id");
    const itemsParam = urlParams.get("items");
    const sessionId = urlParams.get("session_id");

    console.log("URL:", { url: window.location.href });
    console.log("Parameters:", {
      firstName,
      lastName,
      itemsParam,
      priceIdsParam,
      sessionId,
    });

    let items = [];
    try {
      items = JSON.parse(decodeURIComponent(itemsParam)) || [];
    } catch (err) {
      console.error("Error parsing items:", { error: err });
    }

    let priceIds = [];
    if (priceIdsParam) {
      priceIds = decodeURIComponent(priceIdsParam).split(",");
    }

    const fetchSerialKeys = async (maxRetries = 3, retryDelay = 2000) => {
      setLoadingKeys(true);
      setError(null);
      let attempts = 0;

      const tryFetch = async () => {
        try {
          const response = await axios.get(
            `https://xobpfm5d5g.execute-api.ca-central-1.amazonaws.com/prod/getSerialKeys/${sessionId}`
          );
          console.log("Serial Keys Response:", response.data);

          // Sort serial keys based on the number after "F" in the second line
          const sortedSerialKeys = response.data.serialKeys.sort((a, b) => {
            // Extract the second line from serialKey string
            const getSerialNumber = (key) => {
              const lines = key.serialKey.split("\n");
              const secondLine = lines[1] || ""; // Second line (e.g., "7422F0008")
              const match = secondLine.match(/F(\d+)/); // Extract number after "F"
              return match ? parseInt(match[1], 10) : 0; // Convert to number, default to 0 if not found
            };
            return getSerialNumber(a) - getSerialNumber(b);
          });

          setUserData({
            firstName: firstName || "",
            lastName: lastName || "",
            items,
            serialKeys: sortedSerialKeys,
            priceIds,
          });
          setLoadingKeys(false);
        } catch (error) {
          console.error("Error fetching serial keys:", {
            message: error.message,
            status: error.response?.status,
            stack: error.stack,
          });
          if (
            error.response &&
            error.response.status === 404 &&
            attempts < maxRetries
          ) {
            attempts++;
            console.log(`Retry ${attempts}/${maxRetries} in ${retryDelay}ms`);
            setTimeout(tryFetch, retryDelay);
          } else {
            setError(
              error.response?.status === 404
                ? "Serial keys not yet available. Please try again later."
                : "Failed to fetch serial keys. Please try again later."
            );
            setLoadingKeys(false);
            setUserData({
              firstName: firstName || "",
              lastName: lastName || "",
              items,
              serialKeys: [],
              priceIds,
            });
          }
        }
      };

      if (sessionId) {
        tryFetch();
      } else {
        setError("No session ID found.");
        setLoadingKeys(false);
        setUserData({
          firstName: firstName || "",
          lastName: lastName || "",
          items,
          serialKeys: [],
          priceIds,
        });
      }
    };

    fetchSerialKeys();
  }, []);

  const handleDownload = (pdfUrl, pdfTitle) => {
    Swal.fire({
      title: "Download",
      text: `Downloading ${pdfTitle}`,
      icon: "success",
    }).then(() => {
      window.open(pdfUrl, "_blank");
    });
  };

  const shouldShowSerialKey =
    userData.priceIds.includes("price_1PxoiI013t2ai8cxpSKPhDJl") &&
    userData.serialKeys &&
    userData.serialKeys.length > 0;

  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="Purchase-section-2">
          <div className="Purchase-Container">
            <h1 className="primary-heading">Thank You for Your Purchase!</h1>
            <h2 className="primary-heading">
              Welcome To Your Number Guaranteed, {userData.firstName}{" "}
              {userData.lastName}!
            </h2>

            {loadingKeys && <p>Loading serial keys...</p>}

            {error && (
              <Modal show={!!error} onHide={() => setError(null)}>
                <Modal.Header closeButton>
                  <Modal.Title>Error</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                  <p>{error}</p>
                </Modal.Body>
                <Modal.Footer>
                  <Button variant="secondary" onClick={() => setError(null)}>
                    Close
                  </Button>
                </Modal.Footer>
              </Modal>
            )}

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
                      {console.log("Formatting serialKey\n" + key.serialKey)}
                      {key.serialKey || "Generating Serial Key"}
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
                            !userData.items.some((p) => p.price === item.id)
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
