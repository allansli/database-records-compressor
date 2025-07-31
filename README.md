# database-records-compressor
Comparison - Compressing high volume of grouped data into database

This project demonstrates a Python script that compares the performance of storing records in a standard SQLite database versus a database with compressed records. It showcases how compressing data before insertion can lead to significant storage savings and evaluates the trade-offs in terms of write and read performance.

## Features

- **Data Compression:** Uses `zlib` to compress records grouped by date.
- **Parallel Processing:** Leverages `multiprocessing` to perform compression and decompression in parallel, utilizing multiple CPU cores to speed up the process.
- **Performance Comparison:** Provides a clear summary table comparing storage size, write time, and read time between the normal and compressed database approaches.

## How to Run

1.  Ensure you have Python 3 installed.
2.  Clone the repository.
3.  Run the script from your terminal:

    ```bash
    python database_insert.py
    ```

## Output

The script will:
1.  Clean up any old database files.
2.  Generate a configurable number of sample records.
3.  Write the records to a normal SQLite database (`example.db`).
4.  Group, compress, and write the records to a compressed SQLite database (`example-compressed.db`).
5.  Read the records from both databases to measure read performance.
6.  Verify that the data in both databases is identical.
7.  Print a summary table with the final performance metrics.

Example Output:
```
--- Final Summary ---
Results for 10,000,000 records and 1,000 unique groups.
---------------------------------------------------------------------------------------------
| Metric                    | Normal          | Compressed      | Variation                 |
|---------------------------|-----------------|-----------------|---------------------------|
| Storage Size (KB)         | 395184.00       | 103024.00       | 3.84x smaller (-73.93%)   |
| Write Time (s)            | 14.0467         | 9.3553          | -33.40%                   |
| Read Time (s)             | 14.2523         | 7.1820          | -49.61%                   |
---------------------------------------------------------------------------------------------
```
