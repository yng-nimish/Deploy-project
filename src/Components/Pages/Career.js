import React from "react";

import { useState } from "react";
import { useForm } from "react-hook-form";
import { CCard } from "@coreui/react";
import { FiArrowRight } from "react-icons/fi";
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
const EmailData = [
  {
    text: "CEO: ceoyng@icloud.com",
  },
  {
    text: "Editor: editor@yournumberguaranteed.com",
  },
];
const BookData = [
  {
    image: Flag2,
    title: "United States",
    text: (
      <div>
        <p>
          Your Number Guaranteed Inc. <br />
          3909 Witmer Rd. Suite #436 <br />
          Niagara Falls, NY <br />
          USA <br />
          14305
        </p>
      </div>
    ),
  },
  ,
  {
    image: Flag3,
    title: "Canada",
    text: (
      <div>
        <p>
          Your Number Guaranteed Inc.
          <br />
          PO Box 92567, Brampton Mall
          <br />
          Brampton, On <br />
          Canada <br />
          L6W 4R1
        </p>
      </div>
    ),
  },
];

const Career = () => {
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
        text: "Profile Submitted Successfully!",
        icon: "success",
      });
    }
  };
  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="home-banner-container">
          <div className="rowC">
            <div>
              <p className="career-text">
                <h1 className="primary-heading"> CAREERS </h1>
                <h1 className="primary-heading">
                  Reach out to us <br />
                </h1>
                Are you ready to Kickstart your career with Your Number
                Guaranteed Inc. <br />
                Submit your profile along with your resume.
              </p>
            </div>
            <div className="contact-form">
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
                  <label> What role are you applying for ?</label>
                  <input
                    type="text"
                    className="field"
                    placeholder="Enter the role here"
                    name="role"
                    required
                  />
                </div>
                <div className="input-box">
                  <label> Upload your Resume </label>
                  <input
                    type="file"
                    className="field"
                    placeholder="Select a file:"
                    name="file"
                    required
                  />
                </div>
                <div className="input-box">
                  <label>Why do you want to work with us ?</label>
                  <textarea
                    name="message"
                    className="field mess"
                    placeholder="Tell us about yourself"
                    required
                  ></textarea>
                  <button type="submit">Submit Profile</button>
                </div>
              </form>
            </div>
            <br />
            <br />
          </div>
        </div>
      </div>
    </div>
  );
};

export default Career;
