import React from "react";
import "./Footer.css";
import { FaFacebookF } from "react-icons/fa";
import { BsTwitter } from "react-icons/bs";
import { BsYoutube } from "react-icons/bs";
import { SiLinkedin } from "react-icons/si";
import { Link, NavLink } from "react-router-dom";

const Footer = () => {
  return (
    <div>
      <div className="footer-wrapper">
        <div className="footer-section-two">
          <div className="footer-section-columns">
            <ul>
              <li>
                <span>OUR COMPANY</span>
              </li>
              <li>
                <NavLink to="/about">About US</NavLink>
              </li>
              <li>
                <NavLink to="/career">Careers</NavLink>
              </li>
              <li>
                <NavLink to="/guarantee">Our Guarantee</NavLink>
              </li>
            </ul>

            {/*          <span>About Us</span>
            <span>Careers</span>
            <span>Our Guarantee</span>
            <span></span>    */}
          </div>
          <div className="footer-section-columns">
            <ul>
              <li>
                <span>Contact Us</span>
              </li>
              <li>
                <NavLink to="/applications">Application Download</NavLink>
              </li>
              <li>
                <NavLink to="/book">Book</NavLink>
              </li>
              <li>
                <NavLink to="/technical_papers">Technical Papers</NavLink>
              </li>
            </ul>
            {/* <span>CUSTOMER SERVICE </span>
            <span>Contact Us</span>
            <span>Application Download</span>
            <span>Book</span>
            <span>Technical Papers</span>    */}
          </div>
          <div className="footer-section-columns">
            <ul>
              <li>
                <span>PURCHASE ORDERS</span>
              </li>
              <li>
                <NavLink to="/purchase">Get your SUN </NavLink>
              </li>
              <li>
                <NavLink to="/terms">Terms </NavLink>
              </li>
            </ul>
            {/*         <span>PURCHASE ORDERS</span>
            <span>Buy a Token</span>   */}
          </div>
        </div>
        <div className="footer-section-two">
          <div className="footer-section-columns-2">
            <span>Follow Us on</span>
            <span className="footer-icons">
              <NavLink to="https://www.facebook.com/people/YourNumber-Guaranteed/pfbid0wT2HAx2JqwiexswrinRUEdY21hcyyhfMsBNzizHeXKjgMW2YQQkBbgGtjEbjEpjol/">
                <FaFacebookF />
              </NavLink>
              <BsTwitter />

              <BsYoutube />
              <NavLink to="https://ca.linkedin.com/in/yournumber-guaranteed-1089432b8">
                <SiLinkedin />
              </NavLink>
            </span>
          </div>
        </div>
      </div>
      <div className="footer-copyright">
        <span>Copyright © 2024 https://www.yournumberguaranteed.com/</span>
      </div>
    </div>
  );
};

export default Footer;
