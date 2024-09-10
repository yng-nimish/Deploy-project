const { format } = require("date-fns");
const series = "founder";

// Date code calculation
const calculateDateCode = (purchaseDateStr) => {
  const startDate = new Date(2024, 7, 24); // August is month 7 (0-based)
  const purchaseDate = new Date(purchaseDateStr);
  const dateDifference = Math.floor(
    (purchaseDate - startDate) / (1000 * 60 * 60 * 24)
  );
  const startCounter = 8240;
  return startCounter - dateDifference;
};

// Map date code to grid locations
const mapDateCodeToGrid = (dateCode) => {
  const dateCodeStr = String(dateCode).padStart(4, "0");
  return {
    I2: dateCodeStr[0],
    H3: dateCodeStr[1],
    E1: dateCodeStr[2],
    C1: dateCodeStr[3],
  };
};

// Map sales counter to grid locations
const mapSalesCounterToGrid = (salesCounter) => {
  const salesCounterStr = String(salesCounter).padStart(9, "0");
  return {
    I1: salesCounterStr[0],
    G2: salesCounterStr[1],
    D1: salesCounterStr[2],
    B3: salesCounterStr[3],
    B1: salesCounterStr[4],
    D3: salesCounterStr[5],
    H2: salesCounterStr[6],
    A3: salesCounterStr[7],
    A1: salesCounterStr[8],
  };
};
const buyerNameCipher = {
  A: [65, 38],
  B: [10, 89],
  C: [76, 3],
  D: [80, 41],
  E: [17, 97],
  F: [81, 64],
  G: [27, 7],
  H: [45, 98],
  I: [86, 50],
  J: [1, 30],
  K: [97, 85],
  L: [40, 19],
  M: [84, 65],
  N: [23, 54],
  O: [77, 51],
  P: [55, 24],
  Q: [33, 58],
  R: [82, 63],
  S: [5, 49],
  T: [93, 36],
  U: [62, 95],
  V: [60, 20],
  W: [57, 67],
  X: [73, 56],
  Y: [9, 69],
  Z: [35, 44],
};

const ownerNameCipher = {
  A: [81, 18],
  B: [46, 53],
  C: [72, 75],
  D: [12, 37],
  E: [90, 6],
  F: [70, 87],
  G: [31, 32],
  H: [52, 74],
  I: [11, 16],
  J: [85, 47],
  K: [63, 91],
  L: [28, 66],
  M: [96, 43],
  N: [2, 8],
  O: [42, 92],
  P: [78, 59],
  Q: [21, 62],
  R: [99, 25],
  S: [51, 34],
  T: [39, 72],
  U: [28, 13],
  V: [26, 79],
  W: [83, 4],
  X: [29, 68],
  Y: [60, 48],
  Z: [14, 94],
};

const countryCodeMapping = {
  USA: 7,
  Canada: 4,
  Europe: 8,
  Asia: 9,
  "South America": 1,
  Mexico: 2,
  Africa: 5,
  China: 3,
};
const mapNameToGrid = (name, cipher, positions) => {
  const nameParts = name.toUpperCase().split(" ");
  if (nameParts.length < 2)
    throw new Error("Both first name and last name are required");

  const [firstName, lastName] = nameParts;
  const [firstInitial, lastInitial] = [firstName[0], lastName[0]];

  const [firstInitialValue1, firstInitialValue2] = cipher[firstInitial] || [
    0, 0,
  ];
  const [lastInitialValue1, lastInitialValue2] = cipher[lastInitial] || [0, 0];

  const firstInitialDigits = String(firstInitialValue1).split("").map(Number);
  const lastInitialDigits = String(lastInitialValue2).split("").map(Number);

  return {
    [positions[0]]: firstInitialDigits[0] || 0,
    [positions[1]]: firstInitialDigits[1] || 0,
    [positions[2]]: lastInitialDigits[0] || 0,
    [positions[3]]: lastInitialDigits[1] || 0,
  };
};
const get4DigitNumber = (salesCounter, initialCounter) => {
  const offset = salesCounter - initialCounter;
  return String(offset).padStart(4, "0");
};

const assembleSerialNumber = (
  purchaseDate,
  salesCounter,
  buyerName,
  ownerName,
  country,
  series
) => {
  const dateCode = calculateDateCode(purchaseDate);
  const dateCodeMapping = mapDateCodeToGrid(dateCode);
  const salesCounterMapping = mapSalesCounterToGrid(salesCounter);

  const buyerNameMapping = mapNameToGrid(buyerName, buyerNameCipher, [
    "H1",
    "G3",
    "G1",
    "C2",
  ]);
  const ownerNameMapping = mapNameToGrid(ownerName, ownerNameCipher, [
    "C3",
    "I3",
    "B2",
    "A2",
  ]);

  const countryCode = countryCodeMapping[country] || 6;
  const countryCodeMappingFinal = { D2: countryCode };

  const seriesLetter =
    {
      founder: "F",
      builder: "B",
      grandparent: "G",
    }[series.toLowerCase()] || "X";

  const seriesMapping = { E2: seriesLetter };

  const initialSalesCounter = 177402716;
  const incrementedNumber = get4DigitNumber(salesCounter, initialSalesCounter);

  const incrementedNumberMapping = {
    E3: incrementedNumber[0],
    F1: incrementedNumber[1],
    F2: incrementedNumber[2],
    F3: incrementedNumber[3],
  };

  return {
    ...dateCodeMapping,
    ...salesCounterMapping,
    ...buyerNameMapping,
    ...ownerNameMapping,
    ...countryCodeMappingFinal,
    ...incrementedNumberMapping,
    ...seriesMapping,
  };
};

const formatGrid = (serialNumberGrid) => {
  const rows = [
    ["A1", "A2", "A3"],
    ["B1", "B2", "B3"],
    ["C1", "C2", "C3"],
    ["D1", "D2", "D3"],
    ["E1", "E2", "E3"],
    ["F1", "F2", "F3"],
    ["G1", "G2", "G3"],
    ["H1", "H2", "H3"],
    ["I1", "I2", "I3"],
  ];

  const formatRow = (row) => {
    return row
      .map((cell) => `${serialNumberGrid[cell] || " "}`.padEnd(2))
      .join(" ");
  };

  const formattedOutput = [
    rows.slice(0, 3).map(formatRow).join(" "),
    rows.slice(3, 6).map(formatRow).join(" "),
    rows.slice(6).map(formatRow).join(" "),
  ];

  return formattedOutput.join("\n");
};

const generateSerialKey = (
  purchaseDate,
  salesCounter,
  buyerName,
  ownerName,
  country,
  series
) => {
  const serialNumberGrid = assembleSerialNumber(
    purchaseDate,
    salesCounter,
    buyerName,
    ownerName,
    country,
    series
  );
  return formatGrid(serialNumberGrid);
};
