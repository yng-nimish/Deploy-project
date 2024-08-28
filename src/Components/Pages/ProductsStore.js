// price id for you gotta do the work : price_1PoBHG013t2ai8cxq5RtiI4j

const productsArray = [
  {
    id: "price_1PoBHG013t2ai8cxq5RtiI4j",
    title: "You Gotta Do The Work",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1tkuUOcWRZ1ZjfRHNYgfQHoRaN8gUWiNonyaLDIfTdWA/preview",
  },
  {
    id: "price_1PnmqU013t2ai8cxLSkG7hP4",
    title: "Randomness",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1LSCcaw06NQxgqB2GkxeHVbm1pXagYxG84gFAQ6pXJRk/preview",
  },
  {
    id: "price_1PoBYK013t2ai8cxhlvirtoE",
    title: "Why it is Right to Admire The Wright Brothers",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/11HbycLTA4_Qd5VdtzMoN6EcwaW0fWLH2_CC8fYr5goA/preview",
  },
  {
    id: "price_1PoBZJ013t2ai8cxl49DO4VB",
    title: "Footwork",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1Vt-YD15lB6lZrKsfXsSdNj0WI4DjYt-GfYPxVYrYVNY/preview",
  },
  /*{
    id: "price_1PsOzV013t2ai8cxE9vkbeSw",
    title: "Payment Processing Fee",
    price: 0.79,
    pdfUrl: "",
  },
  */
];

function getProductData(id) {
  let productData = productsArray.find((product) => product.id === id);

  if (productData === undefined) {
    console.log("Product Data does not exist for ID: " + id);
    return undefined;
  }
  return productData;
}

export { productsArray, getProductData };
