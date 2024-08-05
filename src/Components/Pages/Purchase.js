import React from "react";

import { useState } from "react";
import { useForm } from "react-hook-form";
import { CCard } from "@coreui/react";
import { FiArrowRight } from "react-icons/fi";
import Flag1 from "../../Assets/Great Britain.svg";
import Flag2 from "../../Assets/USA.svg";
import Flag3 from "../../Assets/Canada.svg";
import { CListGroup } from "@coreui/react";
import { CListGroupItem } from "@coreui/react";
import { CCardBody } from "@coreui/react";
import { CCardTitle } from "@coreui/react";
import { CForm } from "@coreui/react";
import { CCardText } from "@coreui/react";
import { CCardLink } from "@coreui/react";
import { CRow } from "@coreui/react";
import { CCol } from "@coreui/react";
import { CCardImage } from "@coreui/react";
import { CFormInput } from "@coreui/react";
import Swal from "sweetalert2";
const BookData = [
  {
    image: Flag1,
    text: (
      <div>
        <a href="https://www.amazon.co.uk/Your-Number-Guaranteed-Conceived-Year/dp/1738172813/ref=sr_1_1?crid=LKM9G5NDTG4G&keywords=Your+Number+Guaranteed&qid=1701475241&sprefix=your+number+guaranteed%2Caps%2C77&sr=8-1">
          <button className="primary-button">
            {"  "}Buy Now{"  "}
            {"  "}
            {"  "}
            <FiArrowRight />
          </button>
        </a>
      </div>
    ),
  },
  {
    image: Flag2,
    text: (
      <div>
        <a href="https://www.amazon.com/Your-Number-Guaranteed-Conceived-Year/dp/1738172813/ref=sr_1_1?crid=LKM9G5NDTG4G&keywords=Your+Number+Guaranteed&qid=1701475241&sprefix=your+number+guaranteed%2Caps%2C77&sr=8-1">
          <button className="primary-button">
            {"  "}Buy Now{"  "}
            {"  "}
            {"  "}
            <FiArrowRight />
          </button>
        </a>
      </div>
    ),
  },
  {
    image: Flag3,
    text: (
      <div>
        <a href="https://www.amazon.ca/Your-Number-Guaranteed-Conceived-Year/dp/1738172813/ref=sr_1_1?crid=LKM9G5NDTG4G&keywords=Your+Number+Guaranteed&qid=1701475241&sprefix=your+number+guaranteed%2Caps%2C77&sr=8-1">
          <button className="primary-button">
            {"  "}Buy Now{"  "}
            {"  "}
            {"  "}
            <FiArrowRight />
          </button>
        </a>
      </div>
    ),
  },
];

const Purchase = () => {
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
        <div className="home-banner-container">
          <div className="rowC">
            <div className="purchase-text">
              <p>
                <h7 className="primary-heading-welcome"> READY TO BUY </h7>
                <h1 className="primary-heading">Reach out to us</h1> <br />
                Are you ready to pre-purchase our first Launch. The Founder
                Series!!. <br />
                Send us an email to book your slots. <br />
                Scroll down to Buy our book
              </p>
            </div>
            <div className="form">
              <form onSubmit={onSubmit}>
                <div className="input-box">
                  <label> Full Name</label>
                  <input
                    type="text"
                    className="field"
                    placeholder="Enter your Name"
                    name="name"
                    required
                  />
                </div>
                <div className="input-box">
                  <label>Email Address</label>
                  <input
                    type="email"
                    className="field"
                    placeholder="Enter your Email"
                    name="email"
                    required
                  />
                </div>
                <div className="input-box">
                  <label> Subject</label>
                  <input
                    type="text"
                    className="field"
                    placeholder="Enter Subject"
                    name="subject"
                    required
                  />
                </div>
                <div className="input-box">
                  <label>Message</label>
                  <textarea
                    name="message"
                    className="field mess"
                    placeholder="Enter your message"
                    required
                  ></textarea>
                  <button type="submit">Send Message</button>
                </div>
              </form>
            </div>
            <br />
            <br />
          </div>
        </div>
        <div className="Book-section">
          <div className="Book-Container">
            <h1 className="primary-heading-2"> Book links </h1>
            <p className="book-links-text">
              Each year we publish a book on Black Friday. This is a collection
              of our work in the past year, and a look forward to where we want
              to go in the next year. We will always encourage and include
              feedback from our members and partners.
            </p>
          </div>
          <div className="work-section-bottom">
            {BookData.map((data) => (
              <div className="work-section-info">
                <div className="info-boxes-img-container">
                  <img src={data.image} alt="" />
                </div>
                <p>{data.text}</p>
              </div>
            ))}
          </div>
          <div className="partner-card-2">
            <CCard color="white" className="pre-rectangle-2">
              <CRow className="g-0">
                <CCol md={8}>
                  <CCardBody className="pre-rectangle-2">
                    <CCardText></CCardText>
                    <CCardTitle></CCardTitle>
                    <CCardText className="book-links-text-2">
                      All rights reserved. No part of this publication may be
                      reproduced, distributed, or transmitted in any form or by
                      any means, including photocopying, recording, or other
                      electronic or mechanical methods without prior written
                      permission of the author, except in cases of brief
                      quotations embodied in critical reviews and certain other
                      non-commercial uses permitted by copyright law. <br />
                      Permission may be sought at:
                      editor@yournumberguaranteed.com
                    </CCardText>
                  </CCardBody>
                </CCol>
              </CRow>
            </CCard>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Purchase;
