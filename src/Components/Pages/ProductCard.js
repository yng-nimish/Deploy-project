import { Card, Button, Form, Row, Col } from "react-bootstrap";
import { CartContext } from "./CartContext";
import { useContext, useState, useEffect } from "react";

function ProductCard(props) {
  const product = props.product;
  const cart = useContext(CartContext);
  const productQuantity = cart.getProductQuantity(product.id);
  const [inputQuantity, setInputQuantity] = useState(
    productQuantity > 0 ? productQuantity : 1
  );
  // Update inputQuantity when productQuantity changes
  useEffect(() => {
    setInputQuantity(productQuantity > 0 ? productQuantity : 1);
  }, [productQuantity]);

  const handleInputChange = (e) => {
    const value = Math.max(1, Number(e.target.value));
    setInputQuantity(value);

    // Update the cart directly when input changes
    if (value > productQuantity) {
      cart.addOneToCart(product.id, value - productQuantity);
    } else if (value < productQuantity) {
      const difference = productQuantity - value;
      for (let i = 0; i < difference; i++) {
        cart.removeOneFromCart(product.id);
      }
    }
  };

  const handleAddToCart = () => {
    cart.addOneToCart(product.id, inputQuantity); // Pass the input quantity directly
    setInputQuantity(1); // Reset input after adding
  };
  const handleIncrement = () => {
    setInputQuantity((prev) => {
      const newQuantity = prev + 1;
      cart.addOneToCart(product.id, newQuantity - productQuantity); // Add the difference
      return newQuantity;
    });
  };

  const handleDecrement = () => {
    if (inputQuantity > 1) {
      setInputQuantity((prev) => {
        const newQuantity = prev - 1;
        cart.removeOneFromCart(product.id); // Decrement in cart
        return newQuantity;
      });
    }
  };

  console.log(cart.items);
  return (
    <Card>
      <Card.Body>
        <Card.Title>{product.title}</Card.Title>
        <Card.Text>${product.price}</Card.Text>
        {productQuantity > 0 ? (
          <>
            <Form as={Row}>
              <Form.Label column="true" sm="6">
                In Cart: {productQuantity}
              </Form.Label>
              <Col sm="6">
                <Form.Control
                  type="number"
                  value={inputQuantity}
                  onChange={handleInputChange}
                  min="1"
                  style={{ width: "80px", display: "inline-block" }}
                />
                <Button
                  sm="6"
                  onClick={handleIncrement}
                  //      onClick={() => cart.addOneToCart(product.id)}
                  className="mx-2"
                >
                  +
                </Button>
                <Button
                  sm="6"
                  onClick={handleDecrement}
                  //    onClick={() => cart.removeOneFromCart(product.id)}
                  className="mx-2"
                >
                  -
                </Button>
              </Col>
            </Form>
            <Button
              variant="danger"
              onClick={() => cart.deleteFromCart(product.id)}
              className="my-2"
            >
              Remove from Cart
            </Button>
          </>
        ) : (
          <Button
            variant="primary"
            //      onClick={() => cart.addOneToCart(product.id)}
            onClick={handleAddToCart}
          >
            Add To Cart
          </Button>
        )}
      </Card.Body>
    </Card>
  );
}

export default ProductCard;
