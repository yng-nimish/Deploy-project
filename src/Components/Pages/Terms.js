/**
 * Terms Page - Website code
 */
import React from "react";
import { CCard } from "@coreui/react";
import { Link, NavLink } from "react-router-dom";
import { CCardBody } from "@coreui/react";
import { CCardTitle } from "@coreui/react";
import { CCardText } from "@coreui/react";
import { CRow } from "@coreui/react";
import { CCol } from "@coreui/react";
import { CCardImage } from "@coreui/react";
import { FiArrowRight } from "react-icons/fi";

const Terms = () => {
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
                    <CCardText>OUR TERMS</CCardText>
                    <CCardTitle>
                      <h1>Read Our Terms</h1>
                    </CCardTitle>
                    <CCardText>
                      <p>
                        Terms and Conditions of Sales. <br />
                        <br />
                        These terms are written by and for average people to
                        read and understand them. So, we encourage everyone to
                        read them. <br />
                        <br />
                        These terms apply to every person, business or
                        institution that purchases a product or service from
                        Your Number Guaranteed Inc. <br />
                        <br />
                        These terms may change periodically, and you agree to
                        accept these changes. <br />
                        <br />
                        All sales are final. <br />
                        <br />
                        If you are publishing any of our Technical Papers, you
                        can publish it once in any periodical, newsletter, email
                        mailing per US$0.99. You must note it’s origin from Your
                        Number Guaranteed, and Author, copyright date, and an
                        authorization number, which is your sales order number.{" "}
                        The Technical Papers are not to be altered from their
                        original version, or wording. <br />
                        <br />
                        Authors may submit to us Technical Papers for
                        publication on our website. It is our exclusive right to
                        publish or not publish these articles on our website.
                        There is no monetary compensation paid to authors that
                        we publish on our website or our annual book or are
                        published from our partnerships with Technical Papers.
                        If one of these articles is published by others from our
                        website, then there will be no fee paid to the original
                        author. The idea is that Partners may develop
                        applications for our products and IP and submit to us a
                        Technical Paper which will help them sell their idea or
                        product through Technical Papers publications, and that
                        is their benefit, not the 99-cent payment for
                        publication. <br />
                        <br />
                        All Sales less than $50.00 US will be charged the
                        Payment Processing Fee that we are charged from our
                        payment processor. It’s about $0.79 <br />
                        <br />
                        All sales to Canadians, or within Canada are subject to
                        13% HST. <br />
                        <br />
                        Sales to all other counties are sales tax exempt in
                        Canada. The purchaser becomes the importer of record in
                        their corresponding country, and we will supply only the
                        bill of sale as actual documentation, which is created
                        by your Stripe Payment. This can be downloaded at time
                        of sale. <br />
                        <br />
                        If any person, business, group, institution or
                        government body uses our Name, logos, IP, trademarks, in
                        any activities, then they are doing so as a Partner.
                        Anyone can register and become a partner. If you are a
                        Partner, then US$0.99 per person/product/application use
                        per month applies. If you are a partner and
                        self-reporting, then you agree that we may audit your
                        activities and accountings at our discretion. We reserve
                        the right to bar the use of our IP in instances where
                        our values are not in line with our Partners. If you
                        have any questions, please send us an email to ask how
                        it works in your situation. We are glad to work with you
                        because we want things to be affordable and fair.
                        <br />
                        <br /> The purchase of a SUN, or Set of Unique Numbers,
                        allows the owner, or member to have access to a suite of
                        free applications. These applications are limited to use
                        by owner only. The owner of the SUN agrees not to
                        share/sell/rent or publish the free applications with/to
                        anyone else. We reserve the right to limit the number of
                        times a member can download the same applications. In
                        simple terms we want members to be able to have
                        applications on their phone, and or computer and or
                        tablet. If you get a new phone or computer, then you
                        should be able to download applications on it.
                        Essentially, we are trying to limit copyright
                        violations. We will take legal actions against
                        purposeful copyright violations, to recover our costs,
                        lost profits, and seek penalties for people or
                        organizations that intentionally violate our Copyrights.
                        <br />
                        <br />
                        If you purchase a SUN for anyone other than yourself,
                        that SUN belongs exclusively to them. On the purchase
                        page there is a form for a purchaser and owner. If a
                        Purchaser buys a Sun for an Owner (which is a different
                        person), then the Owner is the exclusive owner. The
                        owner of the SUN is responsible to keep it safe and not
                        share it with anyone else. Only share Snippets of your
                        SUN with trusted Partners. <br />
                        <br />
                        If you lose your SUN, then it is not recoverable.
                        <br />
                        <br /> You have read and agreed to our $1,000,000
                        Guarantee. <br />
                        <br />
                        Your Number Guaranteed Inc. is limited in liability to
                        the purchase price of any product or service. Any
                        disputes are to be litigated in the province of Ontario,
                        Canada.
                      </p>
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
              </CRow>
            </CCard>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Terms;
