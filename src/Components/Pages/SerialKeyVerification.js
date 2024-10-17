import React, { useState } from "react";
import { Link, NavLink } from "react-router-dom";
import { FiArrowLeft } from "react-icons/fi";

const SerialKeyVerification = () => {
  const [serialKey, setSerialKey] = useState("");
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const verifySerialKey = async () => {
    setError(null);
    setResult(null);

    if (!serialKey) {
      setError("Serial key is required.");
      return;
    }

    try {
      const response = await fetch("https://your-api-endpoint/verify", {
        // Replace with your actual endpoint
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ serialKey }),
      });

      const data = await response.json();

      if (response.ok) {
        setResult(data.data);
      } else {
        setError(data.error);
      }
    } catch (error) {
      console.error("Error:", error);
      setError("Internal server error.");
    }
  };

  return (
    <div>
      <div>
        <h1>Verify Your Serial Key</h1>
        <input
          type="text"
          value={serialKey}
          onChange={(e) => setSerialKey(e.target.value)}
          placeholder="Enter Serial Key"
          required
        />
        <button onClick={verifySerialKey}>Verify</button>
        {result && (
          <div>
            <h2>Valid Serial Key!</h2>
            <pre>{JSON.stringify(result, null, 2)}</pre>
          </div>
        )}
        {error && <p style={{ color: "red" }}>{error}</p>}
        <br /> <br /> <br />
      </div>
      <div>
        <Link to="/purchaseTP">
          <button color="primary" href="#">
            <FiArrowLeft />
            &nbsp;&nbsp;&nbsp; Go Back &nbsp;&nbsp;&nbsp;
          </button>
        </Link>
      </div>
    </div>
  );
};

export default SerialKeyVerification;
