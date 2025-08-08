// price id for you gotta do the work : price_1PoBHG013t2ai8cxq5RtiI4j
/**
 * E - commerce Products for Purchase Technical Paper section of Purchase Page - Website code
 * We have Live mode and test mode for stripe here.
 */
const productsArray = [
  // Live Mode

  {
    id: "price_1Q5UmO013t2ai8cxX1H3r2IG",
    title: "The One Million Metre Apple Watch Swim Challenge",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1Wtmfk0LYkJLXMjGOJcXtKr99cT54g9FSshmSm9UByN8/export?format=pdf",
  },
  {
    id: "price_1Py2vY013t2ai8cxQdrdWxM0",
    title: "You Gotta Do The Work",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1tkuUOcWRZ1ZjfRHNYgfQHoRaN8gUWiNonyaLDIfTdWA/export?format=pdf",
  },
  {
    id: "price_1Py2va013t2ai8cxEnos9q5z",
    title: "Randomness",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1LSCcaw06NQxgqB2GkxeHVbm1pXagYxG84gFAQ6pXJRk/export?format=pdf",
  },
  {
    id: "price_1Py2vW013t2ai8cxFKmc6Jf6",
    title: "Why it is Right to Admire The Wright Brothers",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/11HbycLTA4_Qd5VdtzMoN6EcwaW0fWLH2_CC8fYr5goA/export?format=pdf",
  },
  {
    id: "price_1Py2vT013t2ai8cx2I8yb7Ek",
    title: "Footwork",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1Vt-YD15lB6lZrKsfXsSdNj0WI4DjYt-GfYPxVYrYVNY/export?format=pdf",
  },
  {
    id: "price_1Qv4Tn013t2ai8cxGNFh6ZcD",
    title: "Do Smart Things",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1C2Cv6Y9vbmeMyh14ayPkzlAK4uI2wPuBWroxlbHuG-8/export?format=pdf",
  },
  {
    id: "price_1R03Zn013t2ai8cxd27UCEVk",
    title: "Are You Nimble",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1l0GoJNx3NUV5s5lpNZTGd4_s136c6QSouT3c8_jxHxs/export?format=pdf",
  },
  {
    id: "price_1R4QpL013t2ai8cxYm3R0B2Q",
    title: "Better Sight Guaranteed",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1fyGk8Lfz05xNSa9Eb2k3n3yLN6uYRs2vzX8tRSZavAA/export?format=pdf",
  },
  {
    id: "price_1RADTH013t2ai8cx08T5KRFj",
    title: "No Phishing from this Dock - Guaranteed",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1BPKSEGWDjMKNsFazrLbk0-jrMDnAfTONmJZYEKI1F5M/export?format=pdf",
  },
  {
    id: "price_1RrQGu013t2ai8cxgAxXmVxQ",
    title: "Never Miss “Your Number Guaranteed” Lotto Selection",
    author: "Kevin Patrick Maloney",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/140RaE-N4ZrWeNHfetlWoX87SUKndd7nOP__EZByVwJo/preview",
  },

  /*
  //Test Mode
  {
    id: "price_1Q5UlK013t2ai8cxjVmKuYBs",
    title: "The One Million Metre Apple Watch Swim Challenge",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1Wtmfk0LYkJLXMjGOJcXtKr99cT54g9FSshmSm9UByN8/export?format=pdf",
  },
  {
    id: "price_1PoBHG013t2ai8cxq5RtiI4j",
    title: "You Gotta Do The Work",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1tkuUOcWRZ1ZjfRHNYgfQHoRaN8gUWiNonyaLDIfTdWA/export?format=pdf",
  },
  {
    id: "price_1PnmqU013t2ai8cxLSkG7hP4",
    title: "Randomness",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1LSCcaw06NQxgqB2GkxeHVbm1pXagYxG84gFAQ6pXJRk/export?format=pdf",
  },
  {
    id: "price_1PoBYK013t2ai8cxhlvirtoE",
    title: "Why it is Right to Admire The Wright Brothers",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/11HbycLTA4_Qd5VdtzMoN6EcwaW0fWLH2_CC8fYr5goA/export?format=pdf",
  },
  {
    id: "price_1PoBZJ013t2ai8cxl49DO4VB",
    title: "Footwork",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1Vt-YD15lB6lZrKsfXsSdNj0WI4DjYt-GfYPxVYrYVNY/export?format=pdf",
  },
  {
    id: "price_1Qv4Wi013t2ai8cxuFdV1BfR",
    title: "Do Smart Things",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1C2Cv6Y9vbmeMyh14ayPkzlAK4uI2wPuBWroxlbHuG-8/export?format=pdf",
  },
  {
    id: "price_1R03hw013t2ai8cxT6AQpyZL",
    title: "Are You Nimble",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1l0GoJNx3NUV5s5lpNZTGd4_s136c6QSouT3c8_jxHxs/export?format=pdf",
  },
  {
    id: "price_1R4QuQ013t2ai8cxvDAAzJPm",
    title: "Better Sight Guaranteed",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1fyGk8Lfz05xNSa9Eb2k3n3yLN6uYRs2vzX8tRSZavAA/export?format=pdf",
  },
  {
    id: "price_1RADVz013t2ai8cxH4kkCZRK",
    title: "No Phishing from this Dock - Guaranteed",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/1BPKSEGWDjMKNsFazrLbk0-jrMDnAfTONmJZYEKI1F5M/export?format=pdf",
  },
  {
    id: "price_1RrQvv013t2ai8cxf07g6u5f",
    title: "Never Miss “Your Number Guaranteed” Lotto Selection",
    author: "Kevin Patrick Maloney",
    price: 0.99,
    pdfUrl:
      "https://docs.google.com/document/d/140RaE-N4ZrWeNHfetlWoX87SUKndd7nOP__EZByVwJo/preview",
  },
  */

  /*{
    id: "price_1PtTgV013t2ai8cxcqb7PFfy",price_1Py2vR013t2ai8cxsp6eOczL
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
