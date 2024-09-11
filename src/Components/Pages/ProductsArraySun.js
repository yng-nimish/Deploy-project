// price id for you gotta do the work : price_1PoBHG013t2ai8cxq5RtiI4j

const productsArraySun = [
  {
    id: "price_1PxoiI013t2ai8cxpSKPhDJl",
    title: "The Founder Series SUN",
    price: 99.99,
    pdfUrl: "",
  },
];

function getProductSunData(id) {
  let productSunData = productsArraySun.find((product) => product.id === id);

  if (productSunData === undefined) {
    console.log("Product SUN Data does not exist for ID: " + id);
    return undefined;
  }
  return productSunData;
}

export { productsArraySun, getProductSunData };
