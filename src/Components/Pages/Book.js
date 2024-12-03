import React from "react";

import { FiArrowRight } from "react-icons/fi";

import BookImage from "../../Assets/Hard Cover Book.svg";
import AurhorImage from "../../Assets/image 4.svg";
import Flag1 from "../../Assets/Great Britain.svg";
import Flag2 from "../../Assets/USA.svg";
import Flag3 from "../../Assets/Canada.svg";
import Book2024Back from "../../Assets/Book2024Back.svg";
import Book2024Front from "../../Assets/Book2024Front 1.svg";

import "../../App.css";
import "bootstrap/dist/css/bootstrap.min.css";
import { CCard } from "@coreui/react";

import { CCardBody } from "@coreui/react";
import { CCardTitle } from "@coreui/react";
import { CCardSubtitle } from "@coreui/react";
import { CCardText } from "@coreui/react";
import { CCardLink } from "@coreui/react";
import { CRow } from "@coreui/react";
import { CCol } from "@coreui/react";
import { CCardImage } from "@coreui/react";
import { Link, NavLink } from "react-router-dom";

const BookData2024 = [
  {
    image: Flag1,
    text: (
      <div>
        <a href="https://www.amazon.co.uk/dp/1738172848/ref=sr_1_2?crid=2IM1FY8YUD18E&dib=eyJ2IjoiMSJ9._yYAiUyh6fHrR6_a5iujWi3Szv-cwgeSlD7VyRUjrRG0fJp5a00DTUCsQZ1DFNiRTzRVNibmo3zULDnzVItSaoQdLzn9e5d0WUZ3hC3xB2F-LF9UDQ2KfGYBTf5HCuqyDqAY2YKG_cSdkp43RWsRFzDnyvz32fpepxqgie54L7fOduSGiaIzRSpBmBWKw8S9QwtlwMgHs19nk9-GNNagoGMIZIJKscoQMN_gVNArmNk.MHafV3pwt5POQtexS8eR0oZuGeAYzIRj0xrlQqbrSVE&dib_tag=se&keywords=your+number+guaranteed&nsdOptOutParam=true&qid=1733147935&sprefix=your+number+guaranteed%2Caps%2C265&sr=8-2">
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
        <a href="https://www.amazon.com/Your-Number-Guaranteed-Birth-Infancy/dp/1738172848/ref=sr_1_1?crid=JQ1Y0RDCLZMK&dib=eyJ2IjoiMSJ9.A9RTI8H-dJNIBvtaHremdA.Od-LgSGZslhEMvYZDqWUCNzaoZQiT2yQDjV3YHKwU9s&dib_tag=se&keywords=your+number+guaranteed+birth+and+infancy+in+the+year+2024&qid=1733147832&sprefix=your+number+guaranteed+birth+and+infancy+in+the+year+2024%2Caps%2C296&sr=8-1">
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
        <a href="https://www.amazon.ca/Your-Number-Guaranteed-Birth-Infancy/dp/1738172848/ref=sr_1_1?crid=2K1VA5949MKI5&dib=eyJ2IjoiMSJ9.NB6UzjFayFBaxq5kdbAVSWLwjU2zZERptfoy2XgTNyQ9LXmMr2tGTmB-m1bLPbcp_8eNX51BpHPUBcAHrC48xESTy1USDgJjH4zAl5sFO8Gwdf6xUBfqLCTItHHDTbtvuRci81pLGhAHtMsBqWSsxmM4uz03W3aciQ4N1Gef4LgLqZMWEPZ2HtwAqnxblFP0jvMCPc9PFl3claatEyUE9otmd_70lJqSMRgS22oCfP-3PfMDUVXrimVxMYfU5IzTKOM_arSddAzDIDpYxiCSBxMzgxix7_t1VWgPY7Q2fSY.rKsv8LaTiqGBoe28BdSqHOECC-5H4R91DTbE2bQB19Q&dib_tag=se&keywords=your+number+guaranteed&qid=1733147623&sprefix=%2Caps%2C281&sr=8-1">
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
const BookData = [
  {
    image: Book2024Front,
  },
  {
    image: Book2024Back,
  },
];
const BookData2023 = [
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

const Book = () => {
  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="book-section-top">
          {BookData.map((data) => (
            <div className="work-section-info">
              <img src={data.image} alt="" />
            </div>
          ))}
        </div>

        <div className="home-banner-container">
          <div></div>
          <div className="home-text-section">
            <h7 className="primary-heading-welcome"> BUY AND READ OUR BOOK </h7>
            <h1 className="primary-heading">You Are Cordially Invited.</h1>
            <p className="primary-text">
              Join the inclusive membership in the community of Your Number
              Guaranteed. Membership is extended to every person who owns a Your
              Number Guaranteed SUN. Members will enjoy a lifetime of free
              benefits. Read our book and you can purchase your <br />
              SUN <FiArrowRight />
              (Set of Unique Numbers) here!
              <br /> <br /> <br />
            </p>
            <div className="button">
              <Link to="/purchase">
                <button className="about-button-2">
                  Get your SUN &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                  <FiArrowRight />
                </button>
              </Link>
            </div>
            <br /> <br /> <br />
          </div>
          <div className="bookImage-container">
            <img src={BookImage} alt="" />
          </div>
        </div>
      </div>
      <div className="partner-section-2">
        <div className="partner-card">
          <CCard color="white" className="mb-3 justify-content-md-centre">
            <CRow className="g-0">
              <CCol md={8}>
                <CCardBody className="pre-rectangle">
                  <CCardText>ABOUT THE AUTHOR</CCardText>
                  <CCardTitle>
                    <h1>Kevin Maloney</h1>
                  </CCardTitle>
                  <CCardText>
                    Kevin Maloney is a veteran of the construction industry
                    who's love of mathematics led him to develop an absolutely
                    unique numerical product which is being used by individual
                    applications of education, entertainment, research,
                    cognitive health, and as a user interface security product.
                    Mathematical fluency allows the author to analyze complex
                    real-world issues and clearly focus on objective
                    understanding of these global issues. The product Your
                    Number Guaranteed is creating a global community, through
                    social media, of thoughtful members who are improving their
                    lives and those around them. <br />
                    <br />
                    <br />
                    <br />
                    <br />
                  </CCardText>
                </CCardBody>
              </CCol>
              <CCol md={4}>
                <CCardImage src={AurhorImage} />
              </CCol>
            </CRow>
          </CCard>
          <CCard color="white" className="mb-3">
            <CRow className="g-0">
              <CCol md={8}>
                <CCardBody className="pre-rectangle">
                  <CCardText>ABOUT THE BOOK</CCardText>
                  <CCardTitle>
                    <h1>Your Number Guaranteed, Conceived in the Year 2023</h1>
                  </CCardTitle>
                  <CCardText>
                    The book describes how a unique mathematical product, which
                    is a true random generated Set of Unique Numbers, which
                    comes with an unheard of One Million Dollar Guarantee, will
                    transform Learning, Education, Cognitive Health, Research,
                    Gaming, Recreational Games, Security and a fundamental
                    understanding of our place in time and community. The book
                    introduces members to a new community in focused social
                    media groups able to reach like minded individuals worldwide
                    in common respectful understanding of our successes and
                    challenges.
                  </CCardText>
                </CCardBody>
              </CCol>
            </CRow>
          </CCard>
        </div>
        <br />
        <br />
        <br />
        <br />
      </div>
      <div className="Book-section">
        <div className="Book-Container">
          <h1 className="primary-heading-2"> Book links to Amazon</h1>
          <p className="book-links-text">
            Each year we publish a book on Black Friday. This is a collection of
            our work in the past year, and a look forward to where we want to go
            in the next year. We will always encourage and include feedback from
            our members and partners.
          </p>
          <h1>2024</h1>
        </div>
        <div className="work-section-bottom">
          {BookData2024.map((data) => (
            <div className="work-section-info">
              <div className="info-boxes-img-container">
                <img src={data.image} alt="" />
              </div>
              <p>{data.text}</p>
            </div>
          ))}
        </div>
        <div className="Book-Container">
          <h1>2023</h1>
        </div>

        <div className="work-section-bottom">
          {BookData2023.map((data) => (
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
                    Permission may be sought at: editor@yournumberguaranteed.com
                  </CCardText>
                </CCardBody>
              </CCol>
            </CRow>
          </CCard>
        </div>
      </div>
    </div>
  );
};

export default Book;
