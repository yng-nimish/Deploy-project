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

  const verifySerialKey = async () => {
    setError(null);
    setLoading(true);

    const formattedSerialKey = serialKey.replace(/\s+/g, "\n").trim();
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

  const handleKeyPress = (e) => {
    if (e.key === "Enter") {
      verifySerialKey();
    }
  };

  return (
    <div className="verify-wrapper">
      <div className="container-fluid">
        <h1 className="primary-heading mb-4">Verify Your Serial Key</h1>
        <p className="lead mb-3">
          Enter the serial key provided on your purchase page to download your
          file.
        </p>
        <div className="instructions-section mb-4">
          <h2 className="h4 mb-3">How to Enter Your Serial Key</h2>
          <ul className="instructions-list">
            <li>
              <strong>Locate Your Serial Key:</strong> You will Find your Serial
              Key on your welcome email that you received after purchasing your
              SUN
              <ul className="serial-key-parts">
                <li>384216707</li>
                <li>1880F0000</li>
                <li>415928043</li>
              </ul>
              (e.g., 384216707 1880F0000 415928043 ).
            </li>
            <li>
              <strong>Format the Serial Key:</strong> Enter the serial key as
              three nine-digit strings, separated by spaces. For example:
              <pre className="serial-key-example mt-2 p-3 bg-light border rounded">
                384216707 1880F0000 415928043
              </pre>
            </li>
            <li>
              <strong>Note:</strong> Ensure each line is a nine-digit string
              with a single space between each string.
            </li>
          </ul>
        </div>
        <div className="form-group mb-4">
          <label htmlFor="serialKeyInput" className="form-label">
            <h2 className="h4 mb-3">Serial Key </h2>
          </label>
          <input
            id="serialKeyInput"
            type="text"
            className={`form-control ${error ? "is-invalid" : ""}`}
            value={serialKey}
            onChange={(e) => setSerialKey(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="Enter your serial key (e.g., 384216707 1880F0000 415928043)"
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

const WrappedSerialKeyVerification = () => {
  return (
    <Authenticator>
      <SerialKeyVerification />
    </Authenticator>
  );
};

export default WrappedSerialKeyVerification;
