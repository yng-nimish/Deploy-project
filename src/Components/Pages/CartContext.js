import { createContext, useState } from "react";
import { productsArray, getProductData } from "./ProductsStore";
import { productsArraySun, getProductSunData } from "./ProductsArraySun";
import { productsArrayIp, getProductIpData } from "./ProductsArrayIp";

export const CartContext = createContext({
  items: [],
  getProductQuantity: () => {},
  addOneToCart: () => {},
  removeOneFromCart: () => {},
  deleteFromCart: () => {},
  getTotalCost: () => {},
  getCartItems: () => {},
});

export function CartProvider({ children }) {
  const [cartProducts, setCartProducts] = useState([]);

  function getCartItems() {
    // Filter out the payment processing fee from cartProducts
    return cartProducts.filter(
      (item) => item.id !== "price_1Py2vR013t2ai8cxsp6eOczL" // Live mode
      //(item) => item.id !== "price_1PtTgV013t2ai8cxcqb7PFfy" // Test mode
    );
  }

  function getProductQuantity(id) {
    const quantity = cartProducts.find(
      (product) => product.id === id
    )?.quantity;

    if (quantity === undefined) {
      return 0;
    }
    return quantity;
  }

  function addOneToCart(id, quantity) {
    if (quantity <= 0) return; // Prevent adding 0 or negative quantities

    const currentQuantity = getProductQuantity(id);

    if (currentQuantity === 0) {
      setCartProducts([
        ...cartProducts,
        {
          id: id,
          quantity: quantity,
        },
      ]);
    } else {
      // product is in cart
      // [ { id: 1 , quantity: 3}, { id: 2, quantity: 1 } ]    add to product id of 2
      setCartProducts(
        cartProducts.map(
          (product) =>
            product.id === id // if condition
              ? { ...product, quantity: product.quantity + quantity } // if statement is true
              : product // if statement is false
        )
      );
    }
  }

  function removeOneFromCart(id) {
    const quantity = getProductQuantity(id);

    if (quantity == 1) {
      deleteFromCart(id);
    } else {
      setCartProducts(
        cartProducts.map(
          (product) =>
            product.id === id // if condition
              ? { ...product, quantity: product.quantity - 1 } // if statement is true
              : product // if statement is false
        )
      );
    }
  }

  function deleteFromCart(id) {
    // [] if an object meeta a condition, add the object to array
    // [product1, product2, product3]
    // [product1, product3]
    setCartProducts((cartProducts) =>
      cartProducts.filter((currentProduct) => {
        return currentProduct.id != id;
      })
    );
  }

  function getTotalCost() {
    let totalCost = 0;
    cartProducts.map((cartItem) => {
      const productData =
        getProductData(cartItem.id) ||
        getProductSunData(cartItem.id) ||
        getProductIpData(cartItem.id);
      totalCost += productData.price * cartItem.quantity;
    });
    // Add payment processing fee if total cost is less than $50
    if (totalCost < 50) {
      totalCost += 0.79; // The fixed payment processing fee in CAD
    }
    return totalCost;
  }

  const contextValue = {
    items: cartProducts,
    getProductQuantity,
    addOneToCart,
    removeOneFromCart,
    deleteFromCart,
    getTotalCost,
  };

  return (
    <CartContext.Provider value={contextValue}>{children}</CartContext.Provider>
  );
}

export default CartProvider;
