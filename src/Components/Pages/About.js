import React from "react";
import { FiArrowRight } from "react-icons/fi";
import BannerImage from "../../Assets/logo2.svg";
import { Link, NavLink } from "react-router-dom";

const about = () => {
  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="home-banner-container">
          <div className="home-text-section">
            <h7 className="primary-heading-welcome"> ABOUT US</h7>
            <h1 className="primary-heading">
              Catalyzing Innovation Through Mathematics
            </h1>
            <p className="primary-text">
              The Set of Unique Number (SUN) is purchased, and the owner will
              receive a lifetime of free useful Applications
              <br /> <br /> <br />
              <br /> <br /> <br />
              <br /> <br /> <br />
            </p>
          </div>
          <div className="home-bannerImage-container">
            <img src={BannerImage} alt="" />
          </div>
        </div>
      </div>
      <div className="about-text-section">
        <div className="about-part-2-text">
          <h1 className="primary-about-heading"> About The SUN </h1>

          <div className="about-section-text-container">
            <p className="primary-text">
              A lifetime membership is obtained with the purchase of a Unique
              Your Number Guaranteed SUN.
              <br />
              Every SUN comes with a $1,000,000 Guarantee that it is Unique (see
              terms).
              <br />
              This Provides unique experiences for every Member with our free
              applications.
              <br />
              Your Number Guaranteed will licence our IP to partners where they
              can pursue their own applications. The most common applications
              are going to be about User Interface Security.
            </p>
            <div className="button">
              <Link to="/purchase">
                <button className="about-button">
                  Get your SUN <FiArrowRight />
                </button>
              </Link>
            </div>
          </div>
        </div>
      </div>
      <div className="about-text-section">
        <div className="about-part-2-text">
          <h1 className="primary-about-heading"> Our Goal</h1>
          <div className="about-section-text-container">
            <p className="primary-text-2">
              Our Goal is to provide educational, cognitive enhancement,
              research & quality control, gaming, entertaining fun games,
              competitions, and User Interface Security Applications. <br />
              We wrote a book about our mission, and we are going to publish an
              updated version every year on Black Friday. BTW, we sent 50 copies
              of this book to 50 esteemed individuals. Who should we send our
              next 50 books to? <br /> We want to translate our books,
              applications, and partnerships into every language that we can.
              <br />
              We believe in math and its abiity to enhance our well-being, and
              to use math to understand our world better. Explore the depths of
              randomness, and its order and insights. <br />
              <br />
            </p>
          </div>
        </div>
      </div>
      <div className="about-text-section">
        <div className="about-part-2-text">
          <h1 className="primary-about-heading"> SUN: The First Step </h1>
          <div className="about-section-text-container">
            <p className="primary-text">
              Your Number Guaranteed sells a set of unique numbers (SUN) and
              provides a variety of Free applications to members:
              <ul className="list-1">
                <li>Education</li>
                <li>Cognitive Health</li>
                <li>Fun Games & Gaming </li>
                <li>Research & Quality Control</li>
                <li>User Interface Security Applications</li>
              </ul>{" "}
              <br />
              Members with a SUN can use the applications for FREE! READ THE
              BOOK for more information on the YNG SUN! $1,000,000 GUARANTEE
              that your SUN is UNIQUE <br />
              <br />
            </p>
            <div className="button">
              <NavLink to="/book">
                <button className="about-button-2">
                  Read The Book <FiArrowRight />
                </button>
              </NavLink>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default about;
