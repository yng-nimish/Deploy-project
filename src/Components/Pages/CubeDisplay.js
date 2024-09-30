import React, { useState, useEffect } from "react";
import { FixedSizeGrid as Grid } from "react-window";
import Papa from "papaparse";

const size = 100; // Size for the cube (100x100x100)

const CubeDisplay = () => {
  const [cube, setCube] = useState([]);
  const [zLayer, setZLayer] = useState(0); // Default z-layer

  useEffect(() => {
    const loadData = async () => {
      try {
        const response = await fetch("/random3digit.csv"); // Adjust the path if needed
        if (!response.ok) {
          throw new Error("Network response was not ok");
        }
        const csvData = await response.text();
        console.log(csvData); // Log the raw CSV data

        // Parse CSV data
        Papa.parse(csvData, {
          complete: (results) => {
            const flatData = results.data.flat().map((value) => {
              const num = parseInt(value.trim(), 10);
              return isNaN(num) ? 0 : num; // Default to 0 if not a number
            });

            console.log("Number of values:", flatData.length); // Log the length of flatData

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
            console.log("Cube data:", cubeData); // Log the cube data
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

  const handleLayerChange = (e) => {
    setZLayer(Number(e.target.value));
  };

  const Cell = ({ columnIndex, rowIndex, style }) => {
    const value = cube[zLayer]?.[rowIndex]?.[columnIndex];
    return (
      <div
        style={{
          ...style,
          border: "1px solid black",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          backgroundColor: "#f0f0f0",
          fontSize: "16px", // Increased font size
          padding: "10px", // Added padding for spacing
          boxSizing: "border-box", // Ensures padding does not affect size
        }}
      >
        {value !== undefined ? String(value).padStart(3, "0") : null}
      </div>
    );
  };

  return (
    <div>
      <h1>3D Cube of Numbers from CSV</h1>
      <label htmlFor="z-layer">Select Z Layer:</label>
      <input
        type="number"
        id="z-layer"
        value={zLayer}
        min="0"
        max="99"
        onChange={handleLayerChange}
      />
      <h2>Layer at Z = {zLayer}</h2>
      <Grid
        columnCount={size}
        columnWidth={50} // Increased column width
        height={400} // Increased height for better visibility
        rowCount={size}
        rowHeight={50} // Increased row height
        width={400} // Increased width for better visibility
      >
        {Cell}
      </Grid>
    </div>
  );
};

export default CubeDisplay;
