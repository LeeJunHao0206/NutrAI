import fs from "fs";
import csv from "csv-parser";
import mysql from "mysql2/promise";
import dotenv from "dotenv";

dotenv.config();

const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
};

const batchSize = 500; // adjust for large datasets

async function importCSV() {
  const connection = await mysql.createConnection(dbConfig);
  console.log("Connected to MySQL Database");

  const filePath = "./data/your_dataset.csv"; // replace with your CSV path
  const table = process.env.TABLE_NAME;

  let batch = [];
  let totalInserted = 0;

  fs.createReadStream(filePath)
    .pipe(csv())
    .on("data", (row) => {
      // Adjust keys to match your CSV headers
      const { name, calories, protein, carbs, fat, serving_size, category, image_url } = row;

      batch.push([name, calories, protein, carbs, fat, serving_size, category, image_url]);

      if (batch.length >= batchSize) {
        insertBatch(connection, table, batch);
        totalInserted += batch.length;
        batch = [];
      }
    })
    .on("end", async () => {
      if (batch.length > 0) {
        await insertBatch(connection, table, batch);
        totalInserted += batch.length;
      }
      await connection.end();
      console.log(`Import complete. Total rows inserted: ${totalInserted}`);
    })
    .on("error", (err) => {
      console.error("Error reading CSV:", err);
    });
}

async function insertBatch(connection, table, rows) {
  const sql = `
    INSERT INTO ${table} (name, calories, protein, carbs, fat, serving_size, category, image_url)
    VALUES ?
  `;
  try {
    await connection.query(sql, [rows]);
    console.log(`Inserted batch of ${rows.length} rows...`);
  } catch (err) {
    console.error("Batch insert error:", err);
  }
}

importCSV();
