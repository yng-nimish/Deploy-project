/**
 * E - commerce Product Page - Website code
 */
import { Button } from "react-bootstrap";
import { CartContext } from "./CartContext";
import { useContext } from "react";
import { getProductData } from "./ProductsStore";
import { getProductSunData } from "./ProductsArraySun";
import { getProductIpData } from "./ProductsArrayIp";

function CartProduct(props) {
  const cart = useContext(CartContext);
  const id = props.id;
  const quantity = props.quantity;
  // Check both arrays for the product
  const productData =
    getProductData(id) || getProductSunData(id) || getProductIpData(id);
  // Handle case where product data might not be found
  if (!productData) {
    return <p>Product data not found for ID: {id}</p>;
  }

  return (
    <>
      <h3>{productData.title}</h3>
      <p>{quantity} total </p>
      <p>${(quantity * productData.price).toFixed(2)}</p>
      <Button size="sm" onClick={() => cart.deleteFromCart(id)}>
        Remove
      </Button>
      <hr></hr>
    </>
  );
}

export default CartProduct;
