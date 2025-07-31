import sqlite3
import zlib
import json
import os
import random
import time
from datetime import date
from collections import defaultdict
from multiprocessing import Pool, cpu_count


# --- Configuration ---
DB_NORMAL = 'example.db'
DB_COMPRESSED = 'example-compressed.db'
NUM_RECORDS = 10000000

# --- Cleanup old database files ---
def cleanup():
    print("Cleaning up old database files...")
    for db_file in [DB_NORMAL, DB_COMPRESSED]:
        if os.path.exists(db_file):
            os.remove(db_file)

# --- Generate Sample Data ---
def generate_records(num):
    print(f"Generating {num} sample records with 1000 unique dates...")
    
    # Create a list of 1000 unique dates to choose from
    possible_dates = set()
    # Generate dates across 3 years to ensure we can get 1000 unique dates
    for year in range(2022, 2025):
        for month in range(1, 13):
            for day in range(1, 29):
                if len(possible_dates) < 1000:
                    possible_dates.add(date(year, month, day))
                else:
                    break
        if len(possible_dates) >= 1000:
            break
    possible_dates = list(possible_dates)

    records = []
    for _ in range(num):
        record = (
            random.choice(possible_dates), # Pick a date from our list
            random.choice(['BUY', 'SELL']),
            ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=4)),
            random.randint(10, 1000),
            round(random.uniform(10.0, 500.0), 2)
        )
        records.append(record)
    return records

# --- Database Operations ---
def setup_databases():
    # Using detect_types to handle DATE columns automatically
    conn_normal = sqlite3.connect(DB_NORMAL, detect_types=sqlite3.PARSE_DECLTYPES)
    c_normal = conn_normal.cursor()
    c_normal.execute('''
        CREATE TABLE stocks (date DATE, trans TEXT, symbol TEXT, qty REAL, price REAL)
    ''')

    conn_compressed = sqlite3.connect(DB_COMPRESSED, detect_types=sqlite3.PARSE_DECLTYPES)
    c_compressed = conn_compressed.cursor()
    c_compressed.execute('CREATE TABLE compressed_data (group_date DATE, data BLOB)')

    return conn_normal, c_normal, conn_compressed, c_compressed

# --- Worker functions for parallel processing ---
def compress_group(group):
    group_date, recs_in_group = group
    # We need to handle date serialization for JSON
    recs_as_str_date = [([r[0].isoformat()] + list(r[1:])) for r in recs_in_group]
    serialized_data = json.dumps(recs_as_str_date).encode('utf-8')
    compressed_data = zlib.compress(serialized_data)
    # Return raw bytes, which are picklable, instead of the non-picklable sqlite3.Binary object
    return (group_date, compressed_data)

def decompress_group(row):
    # row is a tuple (group_date, compressed_data_blob)
    decompressed_data = zlib.decompress(row[1])
    recs_from_blob = json.loads(decompressed_data.decode('utf-8'))
    reconstructed_records = []
    for r in recs_from_blob:
        r[0] = date.fromisoformat(r[0])
        reconstructed_records.append(tuple(r))
    return reconstructed_records

def main():
    cleanup()
    records = generate_records(NUM_RECORDS)
    conn_normal, c_normal, conn_compressed, c_compressed = setup_databases()

    # --- WRITE OPERATIONS ---
    print(f"\n-> Writing {NUM_RECORDS} records to '{DB_NORMAL}'...")
    start_time = time.perf_counter()
    c_normal.executemany('INSERT INTO stocks VALUES (?,?,?,?,?)', records)
    conn_normal.commit()
    time_normal_write = time.perf_counter() - start_time
    print(f"   ... Done in {time_normal_write:.4f} seconds.")
    time.sleep(0.01)

    print(f"\n-> Grouping, compressing (in parallel), and writing to '{DB_COMPRESSED}'...")
    start_time = time.perf_counter()
    
    # 1. Group records (this is fast)
    grouped_records = defaultdict(list)
    for rec in records:
        grouped_records[rec[0]].append(rec)
    
    # 2. Set up a process pool to compress groups in parallel
    num_processes = cpu_count()
    print(f"   ... using {num_processes} CPU cores for compression.")
    with Pool(processes=num_processes) as pool:
        # The map function distributes the work and collects the results (as raw bytes)
        results_from_pool = pool.map(compress_group, grouped_records.items())
    
    # 4. Convert the raw bytes to sqlite3.Binary objects in the main process
    compressed_for_insert = [(res[0], sqlite3.Binary(res[1])) for res in results_from_pool]
    
    # 3. Insert the pre-compressed data
    c_compressed.executemany('INSERT INTO compressed_data VALUES (?, ?)', compressed_for_insert)
    conn_compressed.commit()
    time_compressed_write = time.perf_counter() - start_time
    print(f"   ... Done. {len(grouped_records)} compressed records inserted in {time_compressed_write:.4f} seconds.")
    time.sleep(0.01)

    # --- READ OPERATIONS ---
    print(f"\n-> Reading all {NUM_RECORDS} records from '{DB_NORMAL}'...")
    start_time = time.perf_counter()
    c_normal.execute('SELECT * FROM stocks')
    _ = c_normal.fetchall() # Fetch all results
    time_normal_read = time.perf_counter() - start_time
    print(f"   ... Done in {time_normal_read:.4f} seconds.")
    time.sleep(0.01)

    print(f"\n-> Reading and decompressing (in parallel) all records from '{DB_COMPRESSED}'...")
    start_time = time.perf_counter()
    c_compressed.execute('SELECT group_date, data FROM compressed_data')
    compressed_rows = c_compressed.fetchall()

    with Pool(processes=cpu_count()) as pool:
        # The map function distributes the decompression work
        results_from_pool = pool.map(decompress_group, compressed_rows)

    # Flatten the list of lists into a single list of records
    all_retrieved_records = [item for sublist in results_from_pool for item in sublist]
    time_compressed_read = time.perf_counter() - start_time
    print(f"   ... Done in {time_compressed_read:.4f} seconds.")
    time.sleep(0.01)

    # --- VERIFICATION ---
    print("\n-> Verifying data integrity...")
    if sorted(records) == sorted(all_retrieved_records):
        print("   ... Verification successful: All data matches.")
    else:
        print("   ... Verification FAILED: Data does not match.")

    # --- FINAL RESULTS ---
    conn_normal.close()
    conn_compressed.close()

    # Calculate metrics for the summary table
    size_normal_kb = os.path.getsize(DB_NORMAL) / 1024
    size_compressed_kb = os.path.getsize(DB_COMPRESSED) / 1024

    storage_ratio = size_normal_kb / size_compressed_kb if size_compressed_kb > 0 else 0
    storage_var = (1 - (1 / storage_ratio)) * 100 if storage_ratio > 0 else 0
    storage_summary = f"{storage_ratio:.2f}x smaller (-{storage_var:.2f}%)"

    write_var = ((time_compressed_write / time_normal_write) - 1) * 100
    write_summary = f"{write_var:+.2f}%"

    read_var = ((time_compressed_read / time_normal_read) - 1) * 100
    read_summary = f"{read_var:+.2f}%"

    # Print summary table
    print("\n--- Final Summary ---")
    print(f"Results for {NUM_RECORDS:,} records and {len(grouped_records)} unique groups.")
    header = f"| {'Metric':<25} | {'Normal':<15} | {'Compressed':<15} | {'Variation':<25} |"
    print("-" * len(header))
    print(header)
    print(f"|{'-'*27}|{'-'*17}|{'-'*17}|{'-'*27}|")
    print(f"| {'Storage Size (KB)':<25} | {size_normal_kb:<15.2f} | {size_compressed_kb:<15.2f} | {storage_summary:<25} |")
    print(f"| {'Write Time (s)':<25} | {time_normal_write:<15.4f} | {time_compressed_write:<15.4f} | {write_summary:<25} |")
    print(f"| {'Read Time (s)':<25} | {time_normal_read:<15.4f} | {time_compressed_read:<15.4f} | {read_summary:<25} |")
    print("-" * len(header))

if __name__ == '__main__':
    main()

