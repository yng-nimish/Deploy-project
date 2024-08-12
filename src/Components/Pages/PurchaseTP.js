import React from "react";

import { useState } from "react";

import comingSoon from "../../Assets/Group 62.svg";
import { data } from "./purchase_data";

import Swal from "sweetalert2";
import Navbar from "../Navbar";
import Footer from "../Footer";

const PurchaseTP = () => {
  const [showIframe, setShowIframe] = useState(false);
  const handleItemClick = (pdfUrl) => {
    setPdfUrl(pdfUrl);
    console.log("Url Updated" + pdfUrl);
    setShowIframe(!showIframe);
  };

  const [pdfUrl, setPdfUrl] = useState(null);

  const onSubmit = async (event) => {
    event.preventDefault();
    const formData = new FormData(event.target);

    formData.append("access_key", "c02d1701-ba2d-4c4f-a4ec-39d29ba377c5");

    const object = Object.fromEntries(formData);
    const json = JSON.stringify(object);

    const res = await fetch("https://api.web3forms.com/submit", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: json,
    }).then((res) => res.json());

    if (res.success) {
      Swal.fire({
        title: "Success!",
        text: "Message sent Successfully!",
        icon: "success",
      });
    }
  };
  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="Purchase-section-2">
          <div className="Purchase-Container">
            <h1 className="primary-heading-2"> Purchase Technical Papers </h1>

            <div className="table">
              <table>
                {" "}
                {/* The Table is not spanning correctly when going into mobile mode  
              
                    <button onClick={() => handleItemClick(item.pdfUrl)}>
                      {item.title}
                    </button>
              */}
                <tr>
                  <th>Title - Click to Purchase</th>
                  <th>Author</th>
                  <th>Date</th>
                </tr>
                {data.map((item, index) => (
                  <tr key={index}>
                    <td>
                      <a onClick={() => handleItemClick(item.pdfUrl)}>
                        {item.title}
                      </a>
                    </td>
                    <td>{item.author}</td>
                    <td>{item.Date}</td>
                  </tr>
                ))}
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default PurchaseTP;
