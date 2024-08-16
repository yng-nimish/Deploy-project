import { useState } from "react";
import { productsArray } from "./ProductsStore";
import { Button, Modal } from "react-bootstrap";
import CartProvider from "./CartContext";
import { CartContext } from "./CartContext";
import { useContext } from "react";
import CartProduct from "./CartProduct";
import ProductCard from "./ProductCard";
import { Row, Col } from "react-bootstrap";
import { loadStripe } from "@stripe/stripe-js";

function Store() {
  const [show, setShow] = useState(false);
  const handleClose = () => setShow(false);
  const handleShow = () => setShow(true);

  const cart = useContext(CartContext);

  const productsCount = cart.items.reduce(
    (sum, product) => sum + product.quantity,
    0
  );
  const handleClick = async (e) => {
    const stripe = await loadStripe(
      "REDACTED"
    );
    const { error } = await stripe.redirectToCheckout({
      lineItems: [
        {
          price: "price_1Pn40F013t2ai8cxwVWKX4So",
          quantity: 1,
        },
      ],
      mode: "payment",
      successUrl: "https://yournumberguaranteed.com/career",
      cancelUrl: "https://www.yournumberguaranteed.com/login",
    });
    if (error) {
      console.error("Stripe Checkout error: ", error);
    }
  };

  return (
    <div className="about-wrapper">
      <div className="about-us-container">
        <div className="Purchase-section-2">
          <div className="Purchase-Container">
            <h1 className="primary-heading-2"> Purchase Technical Papers </h1>

            <div className="table">
              <Row xs={1} md={3} className="g-4">
                {productsArray.map((product, idx) => (
                  <Col align="center" key={idx}>
                    <ProductCard product={product} />
                  </Col>
                ))}
              </Row>
            </div>
            <Button onClick={handleShow}>Cart ({productsCount} items )</Button>
            <Button variant="success" onClick={handleClick}>
              {" "}
              Purchase Items!!
            </Button>
          </div>
          <Modal show={show} onHide={handleClose}>
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
                  <h1>Total: {cart.getTotalCost().toFixed(2)}</h1>

                  <Button variant="success">Purchase Items!</Button>
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
