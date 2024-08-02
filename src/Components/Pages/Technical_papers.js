import React from "react";
import { useEffect, useState } from "react";

import { FiArrowRight } from "react-icons/fi";
import { Link, NavLink } from "react-router-dom";

import PDFViewer from "./PDFViewer";
import { data } from "./data";

const Technical_papers = () => {
  const [showIframe, setShowIframe] = useState(false);
  const handleItemClick = (pdfUrl) => {
    setPdfUrl(pdfUrl);
    console.log("Url Updated" + pdfUrl);
    setShowIframe(!showIframe);
  };

  const [pdfUrl, setPdfUrl] = useState(null);

  return (
    <div className="technical-wrapper">
      <div className="technical-container">
        <div className="technical-1">
          <div className="home-text-section">
            <h7 className="primary-heading-welcome"> TECHNICAL PAPERS</h7>
            <h1 className="primary-heading">
              Unlock valuable insights into our vibrant community
            </h1>
            <p className="primary-text">
              Available here for free reading, they offer a glimpse into the
              innovative spirit that defines us. For those seeking to share our
              knowledge, we welcome you to become a partner. Individuals or
              organizations may publish any of our papers which requires
              partnership. You can publish our work for a nominal fee of just
              $0.99 for each publication.
            </p>
          </div>
          <div>
            <p>Your Number Guaranteed Inc. Â© 2024</p>
            <br /> <br /> <br />
            <br /> <br /> <br />
            <br /> <br /> <br />
          </div>
        </div>
      </div>
      <div className="technical-search-section">
        <div className="technical-search">
          <div className="table">
            <table>
              {" "}
              {/* The Table is not spanning correctly when going into mobile mode  
              
                    <button onClick={() => handleItemClick(item.pdfUrl)}>
                      {item.title}
                    </button>
              */}
              <tr>
                <th>Title - Click to Read for FREE</th>
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
            {showIframe && (
              <iframe
                className="frame"
                src={pdfUrl}
                width="800"
                height="400"
                title="Read for Free"
              ></iframe>
            )}
            <br /> <br /> <br />
            <br /> <br /> <br />
          </div>
          <div className="submit-paper">
            <div>
              <Link to="/contact">
                <button className="tech-button">
                  Write a Paper for Submission
                  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                  <FiArrowRight />
                </button>
              </Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Technical_papers;
