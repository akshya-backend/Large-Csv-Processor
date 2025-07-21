const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const csvToCsv = (filePath, saveToFile = true, filterFn = null) => {
  return new Promise((resolve, reject) => {
    // Stats tracking
    let processedRows = 0;
    let filteredRows = 0;
    let headers = [];
    let startTime = Date.now();
    let writeStream = null;
    let csvData = ''; // Only used when saveToFile is false
    
    console.log('🚀 Starting CSV processing with streaming approach...');

    // Setup output stream if saving to file
    if (saveToFile) {
      const outputDir = path.join(process.cwd(), 'output');
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
      }
      
      const outputPath = path.join(outputDir, 'output.csv');
      writeStream = fs.createWriteStream(outputPath, { encoding: 'utf8' });
      
      // Handle write stream errors
      writeStream.on('error', (err) => {
        console.error('❌ Write stream error:', err);
        reject(err);
      });
    }

    // Helper function to escape CSV values
    const escapeCsvValue = (value) => {
      if (value == null) return '';
      const stringValue = String(value);
      
      // If value contains comma, newline, or quote, wrap in quotes and escape quotes
      if (stringValue.includes(',') || stringValue.includes('\n') || stringValue.includes('"')) {
        return `"${stringValue.replace(/"/g, '""')}"`;
      }
      return stringValue;
    };

    // Helper function to write/store CSV row
    const outputRow = (rowData) => {
      const csvRow = rowData + '\n';
      
      if (saveToFile && writeStream) {
        writeStream.write(csvRow);
      } else {
        csvData += csvRow;
      }
    };

    // Progress logging function
    const logProgress = () => {
      const elapsed = (Date.now() - startTime) / 1000;
      const rowsPerSecond = Math.round(processedRows / elapsed);
      const memoryMB = Math.round(process.memoryUsage().rss / 1024 / 1024);
      
      console.log(`📊 Progress: ${processedRows.toLocaleString()} processed, ` +
                 `${filteredRows.toLocaleString()} kept, ` +
                 `${rowsPerSecond}/sec, Memory: ${memoryMB}MB`);
    };

    fs.createReadStream(filePath, { encoding: 'utf8' })
      .pipe(csv())
      .on('headers', (csvHeaders) => {
        headers = csvHeaders;
        console.log('✅ Headers detected:', headers);
        
        // Write headers immediately to output
        const headerRow = headers.map(h => escapeCsvValue(h)).join(',');
        outputRow(headerRow);
      })
      .on('data', (row) => {
        processedRows++;
        
        // Apply filter if provided
        const shouldKeepRow = !filterFn || filterFn(row);
        
        if (shouldKeepRow) {
          // Convert row object back to CSV format immediately
          const csvRowData = headers.map(header => {
            const value = row[header];
            return escapeCsvValue(value);
          }).join(',');
          
          // Write/store row immediately (streaming!)
          outputRow(csvRowData);
          filteredRows++;
          
          // Log accepted row (optional, remove for better performance)
          if (processedRows <= 10) {
            console.log('📥 Row accepted:', row);
          }
        } else {
          // Log filtered out row (optional, remove for better performance)  
          if (processedRows <= 10) {
            console.log('❌ Row filtered out:', row);
          }
        }

        // Progress logging every 50,000 rows
        if (processedRows % 50000 === 0) {
          logProgress();
        }
      })
      .on('end', () => {
        const endTime = Date.now();
        const duration = (endTime - startTime) / 1000;
        const finalMemoryMB = Math.round(process.memoryUsage().rss / 1024 / 1024);

        console.log('🏁 Processing completed!');
        console.log(`📈 Final Stats:`);
        console.log(`   - Total rows processed: ${processedRows.toLocaleString()}`);
        console.log(`   - Rows in output: ${filteredRows.toLocaleString()}`);
        console.log(`   - Duration: ${duration.toFixed(2)} seconds`);
        console.log(`   - Speed: ${Math.round(processedRows / duration).toLocaleString()} rows/sec`);
        console.log(`   - Memory usage: ${finalMemoryMB}MB`);

        if (filteredRows === 0) {
          console.warn('⚠️ No data rows found in output after filtering.');
        }

        if (saveToFile && writeStream) {
          // Close write stream
          writeStream.end();
          
          writeStream.on('finish', () => {
            const outputPath = path.join(process.cwd(), 'output', 'output.csv');
            console.log(`✅ CSV saved to: ${outputPath}`);
            
            resolve({
              success: true,
              outputPath,
              processedRows,
              filteredRows,
              duration,
              memoryUsedMB: finalMemoryMB
            });
          });
        } else {
          // Return CSV data as string
          resolve({
            success: true,
            csvData,
            processedRows,
            filteredRows,
            duration,
            memoryUsedMB: finalMemoryMB
          });
        }
      })
      .on('error', (err) => {
        console.error('❌ Error while reading CSV:', err);
        
        if (writeStream) {
          writeStream.destroy();
        }
        
        reject(err);
      });
  });
};


module.exports = {
  csvToCsv
};