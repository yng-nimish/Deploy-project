/**
 * Application Page - Website code
 */
import React from "react";

import { FiArrowRight } from "react-icons/fi";
import SnipetImage from "../../Assets/snippet.svg";
import comingSoon from "../../Assets/Group 62.svg";
import { Link, NavLink } from "react-router-dom";

import { CCard } from "@coreui/react";
import { CCardImage } from "@coreui/react";
import { CCardBody } from "@coreui/react";
import { CCardTitle } from "@coreui/react";
import { CCardText } from "@coreui/react";
import { CButton } from "@coreui/react";
import { CRow } from "@coreui/react";
import { CCol } from "@coreui/react";
import "bootstrap/dist/css/bootstrap.min.css";
{
  /*
import { CDropdown } from "@coreui/react";
import { CDropdownToggle } from "@coreui/react";
import { CDropdownMenu } from "@coreui/react";
import { CDropdownItem } from "@coreui/react";
import { CDropdownDivider } from "@coreui/react";
import { CButtonGroup } from "@coreui/react";
*/
}
const Applications = () => {
  return (
    <div className="technical-wrapper">
      <div className="technical-container">
        <div className="technical-1">
          <div className="home-text-section">
            <h7 className="primary-heading-welcome">
              {" "}
              APPLICATION DOWNLOADS <br /> <br />
            </h7>
            <h7 className="primary-heading-app">
              To download any application you must be the following:
              <br />
              <br />
            </h7>
            <h3>
              <ul>
                <li>A Member in good standing</li>
                <li>
                  Owner of a Your Number Guaranteed SUN
                  <FiArrowRight /> (Set of Unique Numbers)
                </li>
              </ul>
            </h3>
          </div>
          <div>
            <br /> <br /> <br />
            <br /> <br /> <br />
            <br /> <br /> <br />
          </div>
        </div>
      </div>
      <div className="app-card-section">
        {/*
        <CButtonGroup
          role="group"
          aria-label="Button group with nested dropdown"
        >
          <CDropdown variant="btn-group">
            <CDropdownToggle color="primary">Dropdown</CDropdownToggle>
            <CDropdownMenu>
              <CDropdownItem href="#">Action</CDropdownItem>
              <CDropdownItem href="#">Another action</CDropdownItem>
              <CDropdownItem href="#">Something else here</CDropdownItem>
              <CDropdownDivider />
              <CDropdownItem href="#">Separated link</CDropdownItem>
            </CDropdownMenu>
          </CDropdown>
        </CButtonGroup>
        */}
        <div className="coming-soon-container">
          <div className="balloon">
            <img src={comingSoon} className="ballon-image" />
          </div>
        </div>
        <CCard color="white" className="mb-3 justify-content-md-centre">
          <CRow className="g-0">
            <CCol md={4}>
              <CCardImage src={SnipetImage} />
            </CCol>
            <CCol className="mb-3 pl-3 my-auto mx-auto col-6" md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>
                  <br />
                  <br />
                </CCardTitle>
                <CCardText>
                  The Snippet can be a Point, a Line, a Plane, or a
                  MiniCube.&nbsp;&nbsp;&nbsp;&nbsp;
                  <Link to="/applicationDownload">
                    <CButton color="primary" href="#">
                      Download &nbsp;&nbsp;&nbsp; <FiArrowRight />
                    </CButton>
                  </Link>
                </CCardText>
              </CCardBody>
            </CCol>
          </CRow>
        </CCard>
        <CCard color="white" className="mb-3">
          <CRow className="g-0">
            <CCol md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>Security</CCardTitle>
                <CCardText>
                  Create your own unique security codes. Partnerships with
                  Companies.
                </CCardText>
              </CCardBody>
            </CCol>
            <CCol className="mb-3 pl-3 my-auto mx-auto col-6" md={4}>
              <Link to="/applicationDownload">
                <CButton color="primary" href="#">
                  Download &nbsp;&nbsp;&nbsp; <FiArrowRight />
                </CButton>
              </Link>
            </CCol>
          </CRow>
        </CCard>
        <CCard color="white" className="mb-3">
          <CRow className="g-0">
            <CCol md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>Education</CCardTitle>
                <CCardText>
                  Learn : Counting, Addition, Subtraction, Multiplication,
                  Division at all levels.{" "}
                </CCardText>
              </CCardBody>
            </CCol>
          </CRow>
          <CRow>
            <CCol md={4}>
              <div className="d-grid gap-5 d-md-flex col-6 mx-auto btn text-nowrap">
                <Link to="/applicationDownload">
                  <CButton color="primary">Learning to Count</CButton>{" "}
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Addition</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Subtraction</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Multiplication</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Division</CButton>
                </Link>

                <br />
              </div>
            </CCol>
          </CRow>
          <CRow>
            <CCol md={4}>
              <div className="d-grid gap-5 d-md-flex col-6 mx-auto btn text-nowrap">
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 1</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 2</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 3</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 4</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 5</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 6</CButton>
                </Link>
                <br />
              </div>
            </CCol>
          </CRow>
          <br />
        </CCard>
        <CCard color="white" className="mb-3">
          <CRow className="g-0">
            <CCol md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>Cognitive Health</CCardTitle>
                <CCardText>Game to improve your memory.</CCardText>
              </CCardBody>
            </CCol>
          </CRow>
          <CRow>
            <CCol md={4}>
              <div className="d-grid gap-5 d-md-flex col-6 mx-auto btn text-nowrap">
                <Link to="/applicationDownload">
                  <CButton color="primary">Higher/Lower</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Lower/Higher</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Even/Odd</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Number search</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Ups & Down</CButton>
                </Link>

                <br />
              </div>
            </CCol>
          </CRow>
          <CRow>
            <CCol md={4}>
              <div className="d-grid gap-5 d-md-flex col-6 mx-auto btn text-nowrap">
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 1</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 2</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 3</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 4</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 5</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Level 6</CButton>
                </Link>
                <br />
              </div>
            </CCol>
          </CRow>
          <br />
        </CCard>

        <CCard color="white" className="mb-3">
          <CRow className="g-0">
            <CCol md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>Research and Quality Control</CCardTitle>
                <CCardText>
                  Random Assignment to a Group. Group sizes from 1 to 10. Random
                  selection for quality control. Select from 1 to 1,000,000
                </CCardText>
              </CCardBody>
            </CCol>
            <CCol className="mb-3 pl-3 my-auto mx-auto col-6" md={4}>
              <Link to="/applicationDownload">
                <CButton color="primary" href="#">
                  Download &nbsp;&nbsp;&nbsp; <FiArrowRight />
                </CButton>
              </Link>
            </CCol>
          </CRow>
        </CCard>
        <CCard color="white" className="mb-3">
          <CRow className="g-0">
            <CCol md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>Gaming</CCardTitle>
                <CCardText>
                  This will be done in conjunction with Partners as we need to
                  ensure compliance.
                </CCardText>
              </CCardBody>
            </CCol>
            <CCol className="mb-3 pl-3 my-auto mx-auto col-6" md={4}>
              <Link to="/applicationDownload">
                <CButton color="primary" href="#">
                  Download &nbsp;&nbsp;&nbsp; <FiArrowRight />
                </CButton>
              </Link>
            </CCol>
          </CRow>
        </CCard>
        <CCard color="white" className="mb-3">
          <CRow className="g-0">
            <CCol md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>Fun Games</CCardTitle>
                <CCardText>
                  Enjoyable numeric games to challenge your wit and speed
                </CCardText>
              </CCardBody>
            </CCol>
          </CRow>
          <CRow>
            <CCol md={4}>
              <div className="d-grid gap-5 d-md-flex col-6 mx-auto btn text-nowrap">
                <Link to="/applicationDownload">
                  <CButton color="primary">Bluffer's Poker</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Bingo</CButton>
                </Link>
                <Link to="/applicationDownload">
                  <CButton color="primary">Higher or Lower</CButton>
                </Link>

                <br />
              </div>
            </CCol>
          </CRow>

          <br />
        </CCard>

        <CCard color="white" className="mb-3">
          <CRow className="g-0">
            <CCol md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>Competitions</CCardTitle>
                <CCardText>
                  All the above games and Applications done with speed and
                  accuracy.
                </CCardText>
              </CCardBody>
            </CCol>
            <CCol className="mb-3 pl-3 my-auto mx-auto col-6" md={4}>
              <Link to="/applicationDownload">
                <CButton color="primary" href="#">
                  Download &nbsp;&nbsp;&nbsp; <FiArrowRight />
                </CButton>
              </Link>
            </CCol>
          </CRow>
        </CCard>
        <CCard color="white" className="mb-3">
          <CRow className="g-0">
            <CCol md={8}>
              <CCardBody className="pre-rectangle">
                <CCardTitle>Fun Partnerships with Companies:</CCardTitle>
                <CCardText>
                  We can create Custom Made Programs using our IP. We can create
                  loyalty programs, interactive games, unique engagements and
                  many fun ways to build relationships between your customers
                  and your organization.
                </CCardText>
              </CCardBody>
            </CCol>
            <CCol className="mb-3 pl-3 my-auto mx-auto col-6" md={4}>
              <Link to="/applicationDownload">
                <CButton color="primary" href="#">
                  Download &nbsp;&nbsp;&nbsp; <FiArrowRight />
                </CButton>
              </Link>
            </CCol>
          </CRow>
        </CCard>
      </div>{" "}
    </div>
  );
};

export default Applications;
