// price id for you gotta do the work : price_1PoBHG013t2ai8cxq5RtiI4j
/**
 * E - commerce Products for Purchase The SUN section of Purchase Page - Website code
 * We have Live mode and test mode for stripe here.
 */
const productsArraySun = [
  /*
  {
    // Live Mode
    id: "price_1Py2vL013t2ai8cxo0WMZZHi",
    title: "The Founder Series SUN",
    price: 99.99,
    pdfUrl: "",
  },
  {
    id: "price_bulk_sun_contact",
    title: "The Founder Series SUN",
    price: 99.99,
    pdfUrl: "",
    isBulkPurchase: true,
  },
  */
  // Test Mode

  {
    id: "price_1PxoiI013t2ai8cxpSKPhDJl",
    title: "The Founder Series SUN",
    price: 99.99,
    pdfUrl: "",
  },
  {
    id: "price_bulk_sun_contact",
    title: "The Founder Series SUN",
    price: 99.99,
    pdfUrl: "",
    isBulkPurchase: true,
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
