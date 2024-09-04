import React, { useState, useEffect } from "react";
import { Button } from "react-bootstrap";
import Swal from "sweetalert2";
import Navbar from "../Navbar";
import Footer from "../Footer";
import axios from "axios";
import { useSearchParams } from "react-router-dom";
import { productsArray, getProductData } from "./ProductsStore";

const PurchaseTP = () => {
  const [searchParams] = useSearchParams();
  const [purchasedItems, setPurchasedItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const sessionId = searchParams.get("session_id");

  useEffect(() => {
    if (sessionId) {
      axios
        .get(`/download/${sessionId}`)
        .then((response) => {
          const purchasedProductIds = response.data.items.map(
            (item) => item.id
          );
          // Filter the productsArray to only include purchased items
          const purchasedProducts = productsArray.filter((product) =>
            purchasedProductIds.includes(product.id)
          );
          setPurchasedItems(purchasedProducts);
          setLoading(false);
        })
        .catch((error) => {
          console.error("Error fetching purchased items:", error);
          Swal.fire({
            icon: "error",
            title: "Oops...",
            text: "Something went wrong!",
          });
          setLoading(false);
        });
    } else {
      Swal.fire({
        icon: "warning",
        title: "No session ID",
        text: "Could not find the session ID in the URL.",
      });
      setLoading(false);
    }
  }, [sessionId]);

  const handleDownload = (itemId) => {
    // Use the pdfUrl from the product data
    const product = getProductData(itemId);
    if (product && product.pdfUrl) {
      window.location.href = product.pdfUrl;
    } else {
      Swal.fire({
        icon: "error",
        title: "Download Error",
        text: "Download URL not available for this item.",
      });
    }
  };

  if (loading) {
    return <p>Loading your purchase data...</p>;
  }

  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <Navbar />
        <div className="Purchase-section-2">
          <div className="Purchase-Container">
            {purchasedItems.length > 0 ? (
              <div className="table">
                <table>
                  <thead>
                    <tr>
                      <th>Title</th>
                      <th>Download</th>
                    </tr>
                  </thead>
                  <tbody>
                    {purchasedItems.map((item) => (
                      <tr key={item.id}>
                        <td>{item.title}</td>
                        <td>
                          <Button
                            variant="success"
                            onClick={() => handleDownload(item.id)}
                          >
                            Download
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p>No items purchased or items not found.</p>
            )}
          </div>
        </div>
        <Footer />
      </div>
    </div>
  );
};

export default PurchaseTP;
