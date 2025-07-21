const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const ExcelJS = require('exceljs');

const csvToExcel = async (inputPath, options = {}) => {
  const {
    sheetName = 'Sheet 1',
    batchSize = 10000,        // Process in batches to manage memory
    filterFn = null,          // Optional filter function
    progressInterval = 50000  // Progress logging interval
  } = options;

  // Set consistent output path
  const outputDir = path.join(process.cwd(), 'output');
  const outputPath = path.join(outputDir, 'output.xlsx');

  // Create output directory if it doesn't exist
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  // Stats tracking
  let processedRows = 0;
  let filteredRows = 0;
  let batchNumber = 0;
  const startTime = Date.now();

  console.log('🚀 Starting CSV to Excel conversion with streaming approach...');

  return new Promise((resolve, reject) => {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet(sheetName);
    
    let headers = [];
    let headersSet = false;
    let rowBatch = [];

    // Helper function to log progress
    const logProgress = () => {
      const elapsed = (Date.now() - startTime) / 1000;
      const rowsPerSecond = Math.round(processedRows / elapsed);
      const memoryMB = Math.round(process.memoryUsage().rss / 1024 / 1024);
      
      console.log(`📊 Progress: ${processedRows.toLocaleString()} processed, ` +
                 `${filteredRows.toLocaleString()} added to Excel, ` +
                 `${rowsPerSecond}/sec, Memory: ${memoryMB}MB`);
    };

    // Helper function to process batch and manage memory
    const processBatch = async (batch, isLast = false) => {
      if (batch.length === 0 && !isLast) return;

      console.log(`🔄 Processing batch ${batchNumber} (${batch.length} rows)`);
      
      // Add batch rows to worksheet
      batch.forEach(rowData => {
        worksheet.addRow(rowData);
      });

      batchNumber++;
      
      // Clear batch from memory
      batch.length = 0;

      // Optional: Force garbage collection if available (for very large files)
      if (global.gc && batchNumber % 10 === 0) {
        global.gc();
      }
    };

    const stream = fs.createReadStream(inputPath, { encoding: 'utf8' });

    stream
      .pipe(csv())
      .on('data', async (row) => {
        processedRows++;

        // Set headers on first row
        if (!headersSet) {
          headers = Object.keys(row);
          
          // Set worksheet columns with headers
          worksheet.columns = headers.map(key => ({
            header: key,
            key: key,
            width: 15 // Adjustable column width
          }));
          
          headersSet = true;
          console.log('✅ Headers set:', headers);
        }

        // Apply filter if provided
        const shouldKeepRow = !filterFn || filterFn(row);
        
        if (shouldKeepRow) {
          // Add row to current batch
          rowBatch.push(row);
          filteredRows++;
          
          // Process batch when it reaches batchSize
          if (rowBatch.length >= batchSize) {
            await processBatch(rowBatch);
          }
        }

        // Progress logging
        if (processedRows % progressInterval === 0) {
          logProgress();
        }
      })
      .on('end', async () => {
        try {
          console.log('📝 Processing final batch...');
          
          // Process any remaining rows in the last batch
          if (rowBatch.length > 0) {
            await processBatch(rowBatch, true);
          }

          console.log('💾 Writing Excel file to disk...');
          
          // Write the workbook to file
          await workbook.xlsx.writeFile(outputPath);

          const endTime = Date.now();
          const duration = (endTime - startTime) / 1000;
          const finalMemoryMB = Math.round(process.memoryUsage().rss / 1024 / 1024);

          console.log('🏁 Excel conversion completed!');
          console.log(`📈 Final Stats:`);
          console.log(`   - Total rows processed: ${processedRows.toLocaleString()}`);
          console.log(`   - Rows in Excel: ${filteredRows.toLocaleString()}`);
          console.log(`   - Duration: ${duration.toFixed(2)} seconds`);
          console.log(`   - Speed: ${Math.round(processedRows / duration).toLocaleString()} rows/sec`);
          console.log(`   - Memory usage: ${finalMemoryMB}MB`);
          console.log(`✅ Excel file saved to: ${outputPath}`);

          resolve({
            success: true,
            file: outputPath,
            processedRows,
            filteredRows,
            duration,
            memoryUsedMB: finalMemoryMB,
            message: 'Excel file saved in output folder'
          });

        } catch (error) {
          console.error('❌ Error writing Excel:', error.message);
          reject(error);
        }
      })
      .on('error', (err) => {
        console.error('❌ Stream error:', err.message);
        reject(err);
      });
  });
};


module.exports = {
  csvToExcel,
};