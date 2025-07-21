const express = require('express');
const fs = require('fs');
const path = require('path');
const { csvToExcel } = require('../services/csvToExcel');
const { csvToJson } = require('../services/csvToJson');
const { csvToCsv } = require('../services/csvTocsv');
const convertRoute = express.Router();

convertRoute.get('/:filename', async (req, res, next) => {
  const { filename } = req.params;
  const { format, ...query } = req.query;

  const filePath = path.join(process.cwd(), 'input', filename);

  if (!fs.existsSync(filePath)) {
    return res.status(404).send('File not found');
  }

  try {
    switch (format) {
      case 'json': {
        await csvToJson(filePath);
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Disposition', 'attachment; filename="output.json"');
        res.send(JSON.stringify({message:"file saved in output folder"}));
        break;
      }

      case 'excel': {
         await csvToExcel(filePath);
        res.setHeader('Content-Disposition', 'attachment; filename="output.xlsx"');
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.send(JSON.stringify({mssage:"filered data saved in output folder"}));

        break;
      }

      case 'csv': {
        await csvToCsv(filePath);
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename="filtered.csv"');
        res.send(JSON.stringify({mssage:"filered data saved in output folder"}));
        break;
      }

      default:
        res.status(400).send('Invalid format. Use ?format=json|excel|csv');
    }
  } catch (err) {
    console.log(err)
    console.error('Conversion Error:', err);
    res.status(500).json({ error: 'Internal Server Error', detail: err.message });
    next(err);
  }
});

module.exports = convertRoute;
