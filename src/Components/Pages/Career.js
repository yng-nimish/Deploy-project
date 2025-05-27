/**
 * Career Page - Website code


import React from "react";
import Flag2 from "../../Assets/USA.svg";
import Flag3 from "../../Assets/Canada.svg";

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

    formData.append("access_key", "14e7a7cf-3ae8-4c8c-b522-2590c31e5686"); // Access keys to CEO@yournumberguaranteed.com

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
 */

import React from "react";

const Career = () => {
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
              <a
                href="https://docs.google.com/forms/d/e/1FAIpQLScBLuA13L_ZOhaVCdfw5jVn6vVQc708juc41OBWx1O3Pq4Gyw/viewform?usp=sharing&ouid=114622847374359952266"
                target=""
                rel=""
              >
                <button className="submit-button">Apply Now</button>
              </a>
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
