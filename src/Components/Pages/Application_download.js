import React from "react";
import comingSoon from "../../Assets/Group 62.svg";
import { Link, NavLink } from "react-router-dom";
import { FiArrowLeft } from "react-icons/fi";
import { CButton } from "@coreui/react";
import Swal from "sweetalert2";

const Application_download = () => {
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
        <div className="coming-soon-container">
          <div className="balloon">
            <img src={comingSoon} className="ballon-image" />
          </div>
        </div>
        <div className="home-banner-container">
          <div className="rowC">
            <div>
              <p>
                <h7 className="primary-heading-welcome">
                  {" "}
                  HAVE A NEW IDEA FOR APPLICATIONS{" "}
                </h7>
                <h1 className="primary-heading">Reach out to us</h1>
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
                  <label> Title</label>
                  <input
                    type="text"
                    className="field"
                    placeholder="Enter Title for your Application"
                    name="title"
                    required
                  />
                </div>
                <div className="input-box">
                  <label>Description</label>
                  <textarea
                    name="description"
                    className="field mess"
                    placeholder="Describe your Idea for the Application"
                    required
                  ></textarea>
                  <button type="submit">Submit Idea</button>

                  <Link to="/applications">
                    <button color="primary" href="#">
                      <FiArrowLeft />
                      &nbsp;&nbsp;&nbsp; Go Back &nbsp;&nbsp;&nbsp;
                    </button>
                  </Link>
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

export default Application_download;
