// Navbar.js
import React, { useState } from "react";
import { Link, NavLink } from "react-router-dom";
import "./Navbar.css";
import { BsCart2 } from "react-icons/bs";
import { CSidebar } from "@coreui/react";
import { cilHamburgerMenu } from "@coreui/icons";
const Navbar = () => {
  const [openMenu, setOpenMenu] = useState(false);

  return (
    <nav className="nav">
      {/*   <Link to="/" className="title">
        Website{" "}
      </Link>
      */}
      <div
        className="menu"
        onClick={() => {
          setOpenMenu(!openMenu);
        }}
      >
        <span></span>
        <span></span>
        <span></span>
      </div>

      <div className="navbar-links-container ">
        <ul className={openMenu ? "open" : ""}>
          <li>
            <NavLink to="/Home">Home</NavLink>
          </li>
          <li>
            <NavLink to="/About">About us </NavLink>
          </li>
          <li>
            <NavLink to="/partners">Partners</NavLink>
          </li>
          <li>
            <NavLink to="/Technical_papers">Technical Papers</NavLink>
          </li>
          <li>
            <NavLink to="/applications">Applications</NavLink>
          </li>
          <li>
            <NavLink to="/book">Books & Merchandise</NavLink>
          </li>
          <li>
            <NavLink to="/contact">Contact us</NavLink>
          </li>
          <li>
            <NavLink to="/login">Login/Register</NavLink>
          </li>
          <li>
            <NavLink to="/purchase">
              <button className="primary-button">
                {"  "}
                <BsCart2 className="navbar-cart-icon" /> Get your SUN
              </button>
            </NavLink>
          </li>
        </ul>
      </div>
    </nav>
  );
};

export default Navbar;
