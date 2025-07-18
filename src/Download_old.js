import React, { useState, useEffect } from "react";
import { useLocation, Link } from "react-router-dom";
import { Authenticator, useAuthenticator } from "@aws-amplify/ui-react";
import { Button } from "@mui/material";
import Swal from "sweetalert2";
import InstructionsSection from "./Components/Pages/InstructionsSection";
import {
  CCard,
  CCardBody,
  CCardTitle,
  CCardText,
  CButton,
  CRow,
  CCol,
} from "@coreui/react";
import { FiArrowLeft } from "react-icons/fi";
import "bootstrap/dist/css/bootstrap.min.css";

const Download = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [hasDownloaded, setHasDownloaded] = useState(false);
  const { user } = useAuthenticator((context) => [context.user]);
  const location = useLocation();
  const { serialKey, fileKey } = location.state || {};

  // Extract serial number from serialKey (e.g., F0001 from 7421F0001)
  const serialNumber = serialKey
    ? serialKey.split("\n")[1]?.match(/F\d{4}/)?.[0] || "Unknown"
    : "Unknown";
  // Format filename with space (e.g., F 0001.zip)
  const fileName =
    serialNumber !== "Unknown"
      ? `${serialNumber.replace(/F(\d{4})/, "F $1")}.zip`
      : "Unknown";

  // Retry fetch with exponential backoff
  const fetchWithRetry = async (url, options, retries = 3, delay = 1000) => {
    for (let i = 0; i < retries; i++) {
      try {
        const response = await fetch(url, options);
        if (!response.ok) {
          const data = await response.json();
          throw new Error(data.error || `HTTP ${response.status}`);
        }
        return response;
      } catch (e) {
        if (i === retries - 1) throw e;
        await new Promise((resolve) => setTimeout(resolve, delay * 2 ** i));
      }
    }
  };

  // Check download status on component mount
  useEffect(() => {
    const checkDownloadStatus = async () => {
      if (!fileKey || !serialKey || !user?.signInDetails?.loginId) {
        setError(
          "Missing required information. Please verify your serial key again."
        );
        return;
      }

      try {
        const response = await fetchWithRetry(
          "https://ndrlnyi5yd.execute-api.us-east-1.amazonaws.com/Deploy/GeneratePreSignedURL",
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              fileKey,
              serialKey,
              email: user.signInDetails.loginId,
            }),
          }
        );

        const data = await response.json();

        if (
          response.status === 403 &&
          data.error.includes("already downloaded")
        ) {
          setHasDownloaded(true);
        }
      } catch (error) {
        console.error("Check download status error:", error);
        setError("Failed to verify download status. Please try again later.");
      }
    };

    checkDownloadStatus();
  }, [fileKey, serialKey, user]);

  const handleDownload = async () => {
    setError(null);
    setLoading(true);

    if (!fileKey || !serialKey || !user?.signInDetails?.loginId) {
      setError(
        "Missing required information. Please verify your serial key again."
      );
      setLoading(false);
      return;
    }

    try {
      const response = await fetchWithRetry(
        "https://ndrlnyi5yd.execute-api.us-east-1.amazonaws.com/Deploy/GeneratePreSignedURL",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            fileKey,
            serialKey,
            email: user.signInDetails.loginId,
          }),
        }
      );

      const data = await response.json();

      if (response.ok && data.url && data.downloadId) {
        Swal.fire({
          title: "Download Started",
          text: `Downloading SUN for ${fileName}`,
          icon: "success",
          timer: 1500,
          showConfirmButton: false,
        }).then(async () => {
          // Initiate download
          window.open(data.url, "_blank");

          // Confirm download with retry
          try {
            const confirmResponse = await fetchWithRetry(
              "https://723gmcbiwi.execute-api.us-east-1.amazonaws.com/Deploy/ConfirmDownload",
              {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                },
                body: JSON.stringify({
                  downloadId: data.downloadId,
                  serialKey,
                  email: user.signInDetails.loginId,
                }),
              }
            );

            const confirmData = await confirmResponse.json();

            if (!confirmResponse.ok) {
              throw new Error(
                confirmData.error ||
                  `ConfirmDownload failed with status ${confirmResponse.status}`
              );
            }

            setHasDownloaded(true); // Disable button after confirmation
          } catch (confirmError) {
            console.error("Download confirmation error:", confirmError);
            Swal.fire({
              title: "Confirmation Error",
              text: `Download started, but confirmation failed: ${confirmError.message}. Contact support if needed.`,
              icon: "warning",
            });
          }
        });
      } else {
        throw new Error(data.error || "Failed to generate download URL.");
      }
    } catch (error) {
      console.error("Download error:", error);
      if (error.message.includes("NoSuchKey")) {
        Swal.fire({
          title: "SUN Under Construction",
          text: "Your SUN is under construction. We will notify you when it’s ready to download.",
          icon: "info",
        });
      } else if (error.message.includes("already downloaded")) {
        setHasDownloaded(true);
        Swal.fire({
          title: "Download Limit Reached",
          text: "You have already downloaded your SUN.",
          icon: "info",
        });
      } else {
        setError(error.message || "Failed to initiate download.");
        Swal.fire({
          title: "Error",
          text: error.message || "Failed to initiate download.",
          icon: "error",
        });
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="download-wrapper">
      <div className="container">
        <h1 className="primary-heading">Download Your SUN</h1>
        <p>
          Your serial key has been verified. Use the button below to download
          your SUN: <strong>{fileName}</strong>
        </p>

        {serialKey && (
          <div className="serial-key">
            <h3>Your Serial Key:</h3>
            <pre
              style={{
                whiteSpace: "pre-wrap",
                fontFamily: "Courier New, monospace",
              }}
            >
              {serialKey}
            </pre>
          </div>
        )}
        <InstructionsSection />

        {error && <div className="alert alert-danger">{error}</div>}
        {hasDownloaded && (
          <div className="alert alert-info">
            You have already downloaded your SUN.
          </div>
        )}

        <CCard color="white" className="mb-3">
          <CRow className="g-0">
            <CCol md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>Download SUN</CCardTitle>
                <CCardText>
                  Click below to download your SUN file ({fileName})
                </CCardText>
              </CCardBody>
            </CCol>
            <CCol className="mb-3 pl-3 my-auto mx-auto col-6" md={4}>
              <CButton
                color="primary"
                onClick={handleDownload}
                disabled={loading || hasDownloaded}
              >
                {loading ? "Processing..." : "Download"}
              </CButton>
            </CCol>
          </CRow>
        </CCard>

        <div className="mt-4">
          <Link to="/login">
            <Button className="btn btn-secondary">
              <FiArrowLeft /> Back to Previous Purchases
            </Button>
          </Link>
        </div>
      </div>
    </div>
  );
};

// Wrap Download component with Authenticator
const WrappedDownload = () => {
  return (
    <Authenticator>
      <Download />
    </Authenticator>
  );
};

export default WrappedDownload;
