/**
 * E - commerce Store Page - Website code
 * We have Live mode and test mode for stripe here.
 */
import { useState } from "react";
import { productsArray } from "./ProductsStore";
import { productsArraySun } from "./ProductsArraySun";
import { productsArrayIp } from "./ProductsArrayIp";
import { Button, Modal } from "react-bootstrap";

import { CartContext } from "./CartContext";
import { useContext } from "react";
import CartProduct from "./CartProduct";
import ProductCard from "./ProductCard";
import { Row, Col } from "react-bootstrap";
import { useNavigate } from "react-router-dom";

function Store() {
  const [showCart, setShowCart] = useState(false);
  const [showForm, setShowForm] = useState(false); // State for showing form
  const navigate = useNavigate();
  const cart = useContext(CartContext);

  const productsCount = cart.items.reduce(
    (sum, product) => sum + product.quantity,
    0
  );

  const handlePurchase = () => {
    const sunProductCount = productsArraySun.reduce(
      (count, product) => count + product.quantity,
      0
    );

    navigate("/purchaseform", {
      state: {
        cartItems: cart.items,
        sunProductCount: sunProductCount, // Pass the count here
      },
    });
  };

  const filteredProductsArray = productsArray.filter(
    //(product) => product.id !== "price_1Py2vR013t2ai8cxsp6eOczL" // Live Mode

    (product) => product.id !== "price_1Px8XL013t2ai8cxAOSkYTjB" // Test Mode
  );
  const totalCost = cart.getTotalCost();
  const isFeeApplicable = totalCost < 50.49;
  const feeMessage = isFeeApplicable
    ? "Note : A Processing Fee of $0.79 is added to all orders less than $50.00."
    : "";

  const handleShowCart = () => setShowCart(true);
  const handleCloseCart = () => setShowCart(false);
  const handleShowForm = () => setShowForm(true);
  const handleCloseForm = () => setShowForm(false);

  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="Purchase-section-2">
          <div className="Purchase-Container">
            <h1 className="primary-heading-2"> Purchase Technical Papers </h1>

            <div className="table">
              <Row xs={1} md={3} className="g-4">
                {filteredProductsArray.map((product, idx) => (
                  <Col align="center" key={idx}>
                    <ProductCard product={product} />
                  </Col>
                ))}
              </Row>
            </div>
            <h1 className="primary-heading-2">
              {" "}
              <br />
              Purchase The SUN <br />
            </h1>

            <div className="table">
              <Row xs={1} md={3} className="g-4">
                {productsArraySun.map((product, idx) => (
                  <Col align="center" key={idx}>
                    <ProductCard product={product} />
                  </Col>
                ))}
              </Row>
            </div>
            <h1 className="primary-heading-2">
              {" "}
              <br />
              Purchase IP <br />{" "}
            </h1>
            <div className="table">
              <Row xs={1} md={3} className="g-4">
                {productsArrayIp.map((product, idx) => (
                  <Col align="center" key={idx}>
                    <ProductCard product={product} />
                  </Col>
                ))}
              </Row>
            </div>

            <div>
              <p>All prices in $US</p>
            </div>
            <Button onClick={handleShowCart}>
              Cart ({productsCount} items )
            </Button>
            <Button variant="success" onClick={handlePurchase}>
              {" "}
              {/* Show form on click */}
              Purchase Items!!
            </Button>
          </div>

          {/* Cart Modal */}
          <Modal show={showCart} onHide={handleCloseCart}>
            <Modal.Header closeButton>
              <Modal.Title>Shopping Cart </Modal.Title>
            </Modal.Header>
            <Modal.Body>
              {productsCount > 0 ? (
                <>
                  <p>Items in your cart: </p>
                  {cart.items.map((currentProduct, idx) => (
                    <CartProduct
                      key={idx}
                      id={currentProduct.id}
                      quantity={currentProduct.quantity}
                    ></CartProduct>
                  ))}
                  <h1>Total: ${cart.getTotalCost().toFixed(2)}</h1>
                  {isFeeApplicable && <p>{feeMessage}</p>}
                  <Button variant="success" onClick={handlePurchase}>
                    {" "}
                    {/* Show form on click */}
                    Proceed to Checkout
                  </Button>
                </>
              ) : (
                <h1>There are no items in your cart! </h1>
              )}
            </Modal.Body>
          </Modal>
        </div>
      </div>
    </div>
  );
}

export default Store;
