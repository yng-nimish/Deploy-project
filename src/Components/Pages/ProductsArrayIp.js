const productsArrayIp = [
  {
    // Live Mode
    id: "price_1Q2JFt013t2ai8cxvwjxNO6w",
    title: "Use our Logo",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1X37ok4ruS7-3Xjr2MJ_IfR8LxE5MYZ4d/export?format=pdf",
  },
  {
    id: "price_1Q2JFq013t2ai8cxoDupAfhT",
    title: "Use our IP",
    price: 0.99,
    pdfUrl: "",
  },
  /*
  {
    // Test Mode
    id: "price_1Q2JAh013t2ai8cxysic35Zn",
    title: "Use our Logo",
    price: 0.99,
    pdfUrl: "",
  },
  {
    id: "price_1Q2JC0013t2ai8cxLrf2vafb",
    title: "Use our IP",
    price: 0.99,
    pdfUrl: "",
  },
   */
];

function getProductIpData(id) {
  let productData = productsArrayIp.find((product) => product.id === id);

  if (productData === undefined) {
    console.log("Product Data does not exist for ID: " + id);
    return undefined;
  }
  return productData;
}

export { productsArrayIp, getProductIpData };
