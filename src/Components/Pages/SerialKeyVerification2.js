import React, { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { FiArrowLeft } from "react-icons/fi";
import { Authenticator, useAuthenticator } from "@aws-amplify/ui-react";
import "bootstrap/dist/css/bootstrap.min.css";

const SerialKeyVerification = () => {
  const [serialKey, setSerialKey] = useState("");
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const { user } = useAuthenticator((context) => [context.user]);
  const navigate = useNavigate();

  // In SerialKeyVerification.js
  const verifySerialKey = async () => {
    setError(null);
    setLoading(true);

    // Normalize serial key to match DynamoDB format (with newlines or spaces)
    const formattedSerialKey = serialKey
      .replace(/\s+/g, "\n") // Convert spaces to newlines
      .trim(); // Remove trailing whitespace
    if (!formattedSerialKey) {
      setError("Please enter a valid serial key.");
      setLoading(false);
      return;
    }

    if (!user?.signInDetails?.loginId) {
      setError("You must be logged in to verify a serial key.");
      setLoading(false);
      return;
    }

    try {
      const response = await fetch(
        "https://40b9j7yz02.execute-api.us-east-1.amazonaws.com/Deploy/VerifySerialKey",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            serialKey: formattedSerialKey,
            email: user.signInDetails.loginId,
          }),
        }
      );

      const data = await response.json();

      if (response.ok) {
        navigate("/download", {
          state: { serialKey: formattedSerialKey, fileKey: data.fileKey },
        });
      } else {
        setError(data.error || "Invalid serial key. Please try again.");
      }
    } catch (error) {
      console.error("Verification error:", error);
      setError("Failed to verify serial key. Please try again later.");
    } finally {
      setLoading(false);
    }
  };

  // Handle Enter key press for form submission
  const handleKeyPress = (e) => {
    if (e.key === "Enter") {
      verifySerialKey();
    }
  };

  return (
    <div className="verify-wrapper">
      <div className="container">
        <h1 className="primary-heading">Verify Your Serial Key</h1>
        <p>
          Enter the serial key provided on your purchase page to download your
          file.
        </p>
        <div className="form-group">
          <label htmlFor="serialKeyInput" className="form-label">
            Serial Key
          </label>
          <input
            id="serialKeyInput"
            type="text"
            className={`form-control ${error ? "is-invalid" : ""}`}
            value={serialKey}
            onChange={(e) => setSerialKey(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="Enter your serial key"
            disabled={loading}
          />
          {error && <div className="invalid-feedback">{error}</div>}
        </div>
        <button
          className="btn btn-primary mt-3"
          onClick={verifySerialKey}
          disabled={loading}
        >
          {loading ? "Verifying..." : "Verify"}
        </button>
        <div className="mt-4">
          <Link to="/purchaseTP">
            <button className="btn btn-secondary">
              <FiArrowLeft /> Back to Purchase
            </button>
          </Link>
        </div>
      </div>
    </div>
  );
};

// Wrap the component with Authenticator
const WrappedSerialKeyVerification = () => {
  return (
    <Authenticator>
      <SerialKeyVerification />
    </Authenticator>
  );
};

export default WrappedSerialKeyVerification;
