import React from "react";
import { Link, NavLink } from "react-router-dom";
import { FiArrowRight } from "react-icons/fi";
import BannerImage from "../../Assets/logo2.svg";
import Grid1 from "../../Assets/Frame 819-2.svg";
import Grid2 from "../../Assets/Frame 820-3.svg";
import Grid3 from "../../Assets/Grid33.svg";

import FounderSeriesTokens from "../../Assets/Group 58-2.svg";
import TokensUnleashed from "../../Assets/Property 1=Variant2.svg";
import BecomeaMember from "../../Assets/Group 58.svg";

import EastIcon from "@mui/icons-material/East";

import { CCard } from "@coreui/react";
import { CCardImage } from "@coreui/react";
import { CCardBody } from "@coreui/react";
import { CCardTitle } from "@coreui/react";
import { CCardText } from "@coreui/react";

import { CRow } from "@coreui/react";
import { CCol } from "@coreui/react";

const Home = () => {
  const trendingData = [
    {
      image: FounderSeriesTokens,
      title: "The Founder Series!",
      text: "On SALE AUGUST 24th: Only 10,000 available!",
    },
    {
      image: TokensUnleashed,
      title: "The SUN Unleashed",
      text: "Delivery begins Nov 4th,2024",
    },
    {
      image: BecomeaMember,
      title: "Become a Member",
      text: "Buy now and become a member of the exclusive Founder Series!",
    },
  ];
  const AccessData = [
    {
      image: Grid1,
      title: "",
      text: "",
    },
    {
      image: Grid2,
      title: "",
      text: "",
    },
    {
      image: Grid3,
      title: (
        <div>
          <Link to="/book">
            <button className="Read-more-button">
              Read More
              &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{" "}
              <FiArrowRight />
            </button>
          </Link>
        </div>
      ),
      text: "",
    },
  ];
  return (
    <div>
      <div className="home-container">
        <div className="home-banner-container">
          <div className="home-text-section">
            <h7 className="primary-heading-welcome"> WELCOME TO</h7>
            <h1 className="primary-heading">Your Number Guaranteed</h1>
            <p className="primary-text">
              Every SUN comes with a $1,000,000 Guarantee that it is Unique (see
              terms).
            </p>
            <Link to="/purchase">
              <button className="secondary-button">
                Get your SUN <FiArrowRight />
              </button>
            </Link>
          </div>
          <div className="home-bannerImage-container">
            <img src={BannerImage} alt="" />
          </div>
        </div>
      </div>
      <div>
        <div className="about-section-wrapper">
          <div className="about-section-top">
            <p className="secondary-subheading"> ABOUT US</p>
            <h1 className="primary-heading-2"> Discover Our Offerings</h1>
          </div>

          <CCard
            color="white"
            className="mb-3 about-card"
            style={{ maxWidth: "1200px" }}
          >
            <CRow className="g-0">
              <CCol md={8}>
                <CCardBody className="pre-rectangle">
                  <CCardTitle>Empowering Members</CCardTitle>
                  <CCardTitle>
                    <h1>Free Applications for Members</h1>
                  </CCardTitle>
                  <CCardText>
                    As a Lifetime Member, you gain exclusive access to our range
                    of free applications tailored to enhance your experience.
                    These applications are designed to streamline your
                    interaction with our platform, providing convenience and
                    efficiency at your fingertips. Enjoy a variety of tools and
                    features, all available to enrich your journey with us.
                    <br />
                    All Applications for Lifetime Member will be unique to them
                    because the Applications are driven from each Member's
                    Guaranteed Unique Number Set. <br /> <br /> <br />
                  </CCardText>
                  <CCardText>
                    <Link to="/applications">
                      <button className="about-read-more-button">
                        READ MORE
                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                        {"         "}
                        <EastIcon />
                      </button>
                    </Link>
                  </CCardText>
                </CCardBody>
              </CCol>
              <CCol md={4}>
                <CCardImage src={BannerImage} />
              </CCol>
            </CRow>
          </CCard>
        </div>
      </div>
      <div className="work-section-wrapper">
        <div className="work-section-top">
          <p className="primary-subheading"> WHAT'S TRENDING</p>
          <h1 className="primary-heading"> Latest Update</h1>
        </div>
        <div className="work-section-bottom">
          {trendingData.map((data) => (
            <div className="work-section-info">
              <div className="info-boxes-img-container">
                <img src={data.image} alt="" />
              </div>
              <h2>{data.title}</h2>
              <p>{data.text}</p>
            </div>
          ))}
        </div>
      </div>
      <div className="work-section-wrapper">
        <div className="Access-Container">
          <p className="secondary-subheading"> EXCLUSIVE ACCESS</p>
          <h1 className="primary-heading-2"> Unique Founder Serial Numbers</h1>
        </div>
        <div className="work-section-bottom">
          {AccessData.map((data) => (
            <div className="work-section-info">
              <div className="info-boxes-img-container">
                <img src={data.image} alt="" />
              </div>
              <p>{data.title}</p>
              <p>{data.text}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default Home;
