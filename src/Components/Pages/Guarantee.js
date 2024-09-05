import React from "react";
import { FiArrowRight } from "react-icons/fi";
import "@coreui/coreui/dist/css/coreui.min.css";
import AurhorImage from "../../Assets/image 4.svg";

import "bootstrap/dist/css/bootstrap.min.css";
import { CCard } from "@coreui/react";
import { Link, NavLink } from "react-router-dom";
import { CCardBody } from "@coreui/react";
import { CCardTitle } from "@coreui/react";
import { CCardText } from "@coreui/react";
import { CRow } from "@coreui/react";
import { CCol } from "@coreui/react";
import { CCardImage } from "@coreui/react";

const Guarantee = () => {
  return (
   <div className="about-wrapper">
      <div className="about-us-container">
        <div className="home-banner-container">
          <div className="guarantee-card">
            <CCard color="white" className="mb-3 justify-content-md-centre">
              <br />
              <br />
              <br />
              <CRow className="g-0">
                <CCol md={8}>
                  <CCardBody className="pre-rectangle">
                    <CCardText >OUR GUARANTEE</CCardText>
                    <CCardTitle>
                      <h1>The $1,000,000 Guarantee</h1>
                    </CCardTitle>
                    <CCardText>
                      <p>
                        Your Number Guaranteed is our proof to you that it is
                        only ever Your Number Guaranteed. Your Number is a true
                        random number generated for you and only you. The number
                        is presented in order of X1, Y1,Z1 to X1000, Y1000,
                        Z1000 in three-digit sets. Each Set of Unique Numbers
                        consists of 1,000,000,000 three-digit numbers. Your
                        Number Guaranteed Inc. sells a Set of Unique Numbers
                        (SUN), and we guarantee that your number set and every
                        number set sold by us is unique.{" "}
                      </p>
                      <br />
                      <p>
                        Every Set of Unique Numbers (SUN) sold by Your Number
                        Guaranteed Inc. will be unique. Every person that owns a
                        SUN is 100% Guaranteed that their SUN is unique. There
                        is a verification process if anyone ever wants to
                        challenge us on the authenticity of our Guarantee. There
                        is a process to the challenge and it's not free, but if
                        you win the challenge, then you get the challenge fee
                        back along with $1,000,000.
                      </p>
                      <br />
                      <p>
                        We have an extremely high level of confidence in our
                        process for building the Set of Unique Numbers (SUN),
                        and we have that same level of confidence that every
                        user experience will be unique, as it is always driven
                        from a Set of Unique Numbers or SUN. We are very
                        confident in our levels of security. We can provide to
                        our members and partners customizable security levels as
                        it relates to User Interface Security Applications.
                        These are highly flexible tools which can be modified by
                        the user, in their circumstances, to provide varying
                        levels of security to their particular application.
                      </p>
                      <br />
                      <p>
                        Our Guarantee is rock solid, our process of building
                        each SUN is rock solid, and our security system is rock
                        solid, so we offer this peace of mind, and we offer our
                      </p>
                      <br />
                      <h3>$1,000,000 Guarantee!!!!</h3>
                      <br />
                      <div className="button">
                        <Link to="/purchase">
                          <button className="about-button-2">
                            Get your SUN &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                            <FiArrowRight />
                          </button>
                        </Link>
                        <br />
                        <br />
                      </div>
                    </CCardText>
                  </CCardBody>
                </CCol>
                <CCol md={4}>
                  <CCardImage src={AurhorImage} />
                  <br />
                  <br />
                  <br />
                  <br />
                </CCol>
              </CRow>
            </CCard>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Guarantee;
