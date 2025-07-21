const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');

const csvToJson = (inputCsvPath) => {
  const outputDir = path.join(process.cwd(), 'output');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const outputJsonPath = path.join(outputDir, 'output.json');

  return new Promise((resolve, reject) => {
    const inputStream = fs.createReadStream(inputCsvPath, { highWaterMark: 1024 * 1024 });
    const outputStream = fs.createWriteStream(outputJsonPath, { encoding: 'utf8' });

    let headers = [];
    let isFirst = true;
    let processed = 0;
    let skipped = 0;
    let line = 0;
    let lastMemory = 0;
    let lastReport = Date.now();

    const reportProgress = () => {
      const now = Date.now();
      if (now - lastReport > 5000) {
        const mem = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
        console.log(`📊 Processed: ${processed.toLocaleString()} | Skipped: ${skipped} | Memory: ${mem} MB`);
        lastMemory = mem;
        lastReport = now;
      }
    };

    outputStream.write('[\n');

    const parser = parse({
      skip_empty_lines: true,
      relax_column_count: true,
      max_record_size: 5 * 1024 * 1024,
    });

    const closeStreams = () => {
      outputStream.write('\n]\n');
      outputStream.end();
    };

    const handleError = (err) => {
      console.error('❌ Error at line', line, ':', err.message);
      inputStream.destroy();
      parser.destroy();
      outputStream.destroy();
      reject(err);
    };

    parser.on('readable', function () {
      let record;
      while ((record = parser.read())) {
        line++;
        try {
          if (line === 1) {
            headers = record.map(h => String(h || '').trim());
            continue;
          }

          if (record.length !== headers.length) {
            skipped++;
            continue;
          }

          const obj = {};
          for (let i = 0; i < headers.length; i++) {
            obj[headers[i]] = record[i] ?? '';
          }

          const jsonLine = (isFirst ? '' : ',\n') + '  ' + JSON.stringify(obj);
          isFirst = false;

          if (!outputStream.write(jsonLine)) {
            parser.pause();
            outputStream.once('drain', () => parser.resume());
          }

          processed++;
          if (processed % 10000 === 0) reportProgress();
        } catch (err) {
          console.warn(`⚠️ Skipping line ${line} due to error: ${err.message}`);
          skipped++;
        }
      }
    });

    parser.on('end', () => {
      closeStreams();
    });

    parser.on('error', handleError);
    inputStream.on('error', handleError);
    outputStream.on('error', handleError);

    outputStream.on('finish', () => {
      const finalSize = fs.statSync(outputJsonPath).size;
      console.log(`✅ Done: ${processed} records written | Skipped: ${skipped}`);
      console.log(`📁 Output: ${outputJsonPath} (${(finalSize / 1024 / 1024).toFixed(2)} MB)`);
      resolve({ processed, skipped, outputFile: outputJsonPath });
    });

    inputStream.pipe(parser);
  });
};

module.exports = { csvToJson };
