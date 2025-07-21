# Large-Csv-Processor

A high-performance Node.js-based CSV conversion API that streams large CSV files (up to 3GB) and converts them to:

- JSON (`.json`)
- Excel (`.xlsx`)
- Filtered CSV (`.csv`)

Supports streaming with low memory usage and output saved to the `output/` folder.

---

## 🚀 Features

- Handles CSV files up to 3GB+ without crashing
- Streams processing to avoid memory spikes
- Converts to JSON, CSV, Excel formats
- Automatic progress logging and memory usage
- Graceful error handling and cleanup

---

## 📡 API Usage via `curl`

Use the following `curl` commands to test the API endpoints locally:

```bash
# Convert input.csv to CSV format (filtered)
curl "http://localhost:3001/api/convert/input.csv?format=csv"
# Output:
# {"message":"filtered data saved in output folder"}

# Convert input.csv to Excel format
curl "http://localhost:3001/api/convert/input.csv?format=excel"
# Output:
# {"message":"filtered data saved in output folder"}

# Convert input.csv to JSON format
curl "http://localhost:3001/api/convert/input.csv?format=json"
# Output:
# {"message":"filtered data saved in output folder"}
