import React, { useState, useEffect } from "react";
import { FixedSizeGrid as Grid } from "react-window";
import Papa from "papaparse";

const size = 100; // Size for the cube (100x100x100)

const CubeDisplay = () => {
  const [cube, setCube] = useState([]);
  const [zLayer, setZLayer] = useState(0);
  const [xLayer, setXLayer] = useState(0);
  const [yLayer, setYLayer] = useState(0);
  const [plane, setPlane] = useState("XY"); // Default plane  // this plane is what will be shown initially when the person comes to the page

  useEffect(() => {
    const loadData = async () => {
      try {
        const response = await fetch(
          "https://raw.githubusercontent.com/yng-nimish/Deploy-project/refs/heads/main/public/random3digit.csv" //This is for Github
          // create a new way of fetching it from amazon s3 bucket
        );
        if (!response.ok) {
          throw new Error("Network response was not ok");
        }
        const csvData = await response.text();

        // Parse CSV data
        Papa.parse(csvData, {
          complete: (results) => {
            const flatData = results.data.flat().map((value) => {
              const num = parseInt(value.trim(), 10);
              return isNaN(num) ? 0 : num; // Default to 0 if not a number
            });

            if (flatData.length !== size * size * size) {
              console.error(
                "Data does not match expected size. Found:",
                flatData.length
              );
              return;
            }

            // Reshape into a 3D array
            const cubeData = Array.from({ length: size }, () =>
              Array.from({ length: size }, () => Array(size))
            );

            let index = 0;
            for (let z = 0; z < size; z++) {
              for (let y = 0; y < size; y++) {
                for (let x = 0; x < size; x++) {
                  cubeData[z][y][x] = flatData[index++];
                }
              }
            }

            setCube(cubeData);
            console.log(cubeData);
          },
          error: (error) => {
            console.error("Error parsing CSV:", error);
          },
        });
      } catch (error) {
        console.error("Error fetching CSV:", error);
      }
    };

    loadData();
  }, []);

  const handlePlaneChange = (e) => {
    setPlane(e.target.value);
  };

  const Cell = ({ value, style }) => (
    <div
      style={{
        ...style,
        border: "1px solid black",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        backgroundColor: "#f0f0f0",
        fontSize: "16px",
        padding: "10px",
        boxSizing: "border-box",
      }}
    >
      {value !== undefined ? String(value).padStart(3, "0") : null}
    </div>
  );

  const renderGrid = () => {
    let rowCount, columnCount, data;

    switch (plane) {
      case "XY":
        rowCount = size * 10;
        columnCount = size * 10;
        data = Array.from({ length: size }, (_, y) =>
          Array.from({ length: size }, (_, x) => cube[zLayer]?.[y]?.[x])
        );
        break;
      case "YZ":
        rowCount = size;
        columnCount = size;
        data = Array.from({ length: size }, (_, y) =>
          Array.from({ length: size }, (_, z) => cube[z]?.[y]?.[xLayer])
        );
        break;
      case "XZ":
        rowCount = size;
        columnCount = size;
        data = Array.from({ length: size }, (_, z) =>
          Array.from({ length: size }, (_, x) => cube[z]?.[yLayer]?.[x])
        );
        break;
      default:
        return null; // Just in case
    }

    return (
      <Grid
        columnCount={columnCount}
        columnWidth={50}
        height={2000}
        rowCount={rowCount}
        rowHeight={50}
        width={10000}
      >
        {({ columnIndex, rowIndex, style }) => (
          <Cell value={data[rowIndex]?.[columnIndex]} style={style} />
        )}
      </Grid>
    );
  };

  return (
    <div>
      <h1>3D Cube of Numbers from CSV</h1>
      <label htmlFor="plane-select">Select Plane:</label>
      <select id="plane-select" value={plane} onChange={handlePlaneChange}>
        <option value="XY">XY Plane</option>
        <option value="YZ">YZ Plane</option>
        <option value="XZ">XZ Plane</option>
      </select>
      <br />
      <label htmlFor="x-layer">Select X Layer:</label>
      <input
        type="number"
        id="x-layer"
        value={xLayer}
        min="0"
        max="99"
        onChange={(e) => setXLayer(Number(e.target.value))}
      />
      <br />
      <label htmlFor="y-layer">Select Y Layer:</label>
      <input
        type="number"
        id="y-layer"
        value={yLayer}
        min="0"
        max="99"
        onChange={(e) => setYLayer(Number(e.target.value))}
      />
      <br />
      <label htmlFor="z-layer">Select Z Layer:</label>
      <input
        type="number"
        id="z-layer"
        value={zLayer}
        min="0"
        max="99"
        onChange={(e) => setZLayer(Number(e.target.value))}
      />
      <h2>
        Viewing {plane} Plane with Layers: X = {xLayer}, Y = {yLayer}, Z ={" "}
        {zLayer}
      </h2>
      {renderGrid()}
    </div>
  );
};

export default CubeDisplay;
