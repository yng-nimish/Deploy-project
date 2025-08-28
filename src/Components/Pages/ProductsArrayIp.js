/**
 * E - commerce Products for Purchase IP section of Purchase Page - Website code
 * We have Live mode and test mode for stripe here.
 */
const productsArrayIp = [
  /*
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
    {
    id: "price_1S0rBE013t2ai8cxpuV1P94I",
    title: "Use our K cube",
    price: 0.99,
    pdfUrl: "",
  },
    {
    id: "price_1S0rCe013t2ai8cxo9B3fAsu",
    title: "Use our F cube",
    price: 0.99,
    pdfUrl: "",
  },
    {
    id: "price_1S1AQz013t2ai8cxbTzKChWo",
    title: "Use Doing Smart Things",
    price: 0.99,
    pdfUrl: "",
  },
*/
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
  {
    id: "price_1S0rEz013t2ai8cx31Elh67h",
    title: "Use our K cube",
    price: 0.99,
    pdfUrl: "",
  },
  {
    id: "price_1S0rFg013t2ai8cxmDJezoN6",
    title: "Use our F cube",
    price: 0.99,
    pdfUrl: "",
  },
  {
    id: "price_1S1AOu013t2ai8cxj419QiUs",
    title: "Use Doing Smart Things",
    price: 0.99,
    pdfUrl: "",
  },
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
