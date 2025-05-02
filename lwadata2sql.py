## lwadata2sql.py
## python lwadata2sql.py --start 2025-04-01T00:00:00 --end 2025-05-01T00:00:00
## python lwadata2sql.py --start 2025-04-30T00:00:00 --end 2025-05-01T00:00:00 --delete

import mysql.connector
import os
from glob import glob
from datetime import datetime, timedelta
import argparse

##=========================connect to database
def create_lwa_query_db_connection():
    return mysql.connector.connect(
        host=os.getenv('FLARE_DB_HOST'),
        database='lwa_metadata_query',#os.getenv('FLARE_DB_DATABASE'),
        user=os.getenv('FLARE_DB_USER'),
        password=os.getenv('FLARE_DB_PASSWORD')
    )
# connection = create_lwa_query_db_connection()
# cursor = connection.cursor()

##=========================
def parse_obs_time(filepath, file_type):
    filename = os.path.basename(filepath)
    try:
        if file_type in ['slow_lev1', 'slow_lev15']:
            date_str = filename.split('T')[0].split('.')[-1]
            time_str = filename.split('T')[1][:6]
            return datetime.strptime(date_str + time_str, "%Y-%m-%d%H%M%S")
        elif file_type == 'spec_fits':
            date_str = filename.split('.')[0]
            return datetime.strptime(date_str, "%Y%m%d")
    except Exception:
        return None

def get_path_lwa_files(timerange):
    start = datetime.strptime(timerange[0], "%Y-%m-%dT%H:%M:%S")
    end = datetime.strptime(timerange[1], "%Y-%m-%dT%H:%M:%S")

    start_1daybf = start - timedelta(days=1)
    end_1dayaf = end + timedelta(days=1)
    # Gather all files from plausible folders (±1 day)
    date_cursor = start_1daybf
    # fast_files, slow_files = [], []
    slow_lev1_files, slow_lev15_files = [], []
    while date_cursor <= end_1dayaf:
        y, m, d = date_cursor.strftime("%Y"), date_cursor.strftime("%m"), date_cursor.strftime("%d")
        for disk in ['nas6', 'nas7']:
            # fast_files += glob(f"/{disk}/ovro-lwa-data/hdf/fast/lev1/{y}/{m}/{d}/*_mfs_*.hdf")
            slow_lev1_files += glob(f"/{disk}/ovro-lwa-data/hdf/slow/lev1/{y}/{m}/{d}/*_mfs_*.hdf")
            slow_lev15_files += glob(f"/{disk}/ovro-lwa-data/hdf/slow/lev15/{y}/{m}/{d}/*_mfs_*.hdf")
        date_cursor += timedelta(days=1)

    spec_candidates = []
    for disk in ['common']:
        spec_candidates += glob(f"/{disk}/lwa/spec_v2/fits/*.fits")

    # Helper to print new dates during filtering
    def filter_and_log(files, file_type):
        print(f"file_type: {file_type}")
        last_day = None
        result = []
        for f in sorted(files):
            t = parse_obs_time(f, file_type)
            if not t or not (start <= t <= end):
                continue
            current_day = t.strftime("%Y-%m-%d")
            if current_day != last_day:
                print(current_day)
                last_day = current_day
            result.append((f, t))
        return result

    spec_filtered = filter_and_log(spec_candidates, 'spec_fits')
    slow_lev1_filtered = filter_and_log(slow_lev1_files, 'slow_lev1')
    slow_lev15_filtered = filter_and_log(slow_lev15_files, 'slow_lev15')
    # fast_filtered = filter_and_log(fast_files, 'fast_hdf')
    # slow_filtered = filter_and_log(slow_files, 'slow_hdf')
    spec_sorted = [f for f, _ in spec_filtered]
    slow_lev1_sorted = [f for f, _ in slow_lev1_filtered]
    slow_lev15_sorted = [f for f, _ in slow_lev15_filtered]
    # fast_sorted = [f for f, _ in fast_filtered]
    return spec_sorted, slow_lev1_sorted, slow_lev15_sorted

# spec, slow, fast = get_path_lwa_files(['2024-12-28T00:00:00', '2024-12-30T00:00:00'])
# print(f"Spec FITS: {len(spec)} found")
# print(f"Slow HDF: {len(slow)} found")
# print(f"Fast HDF: {len(fast)} found")


def insert_file_list_to_mysql(file_list, file_type, batch_size=1000):
    # table_map = {
    #     'fast_hdf': 'lwa_fast_hdf_files',
    #     'slow_hdf': 'lwa_slow_hdf_files',
    #     'spec_fits': 'lwa_spec_fits_files'
    # }
    table_map = {
        'slow_lev1': 'lwa_slow_lev1_hdf_files',
        'slow_lev15': 'lwa_slow_lev15_hdf_files',
        'spec_fits': 'lwa_spec_fits_files'
    }

    table = table_map[file_type]
    inserted, skipped = 0, 0
    batch = []
    connection = create_lwa_query_db_connection()
    cursor = connection.cursor()

    for i, file_path in enumerate(file_list):
        obs_time = parse_obs_time(file_path, file_type)
        if not obs_time:
            skipped += 1
            continue

        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE file_path = %s", (file_path,))

        if cursor.fetchone()[0] == 0:
            batch.append((file_path, obs_time))
            inserted += 1
        else:
            skipped += 1

        # If batch is full, insert and commit
        if len(batch) >= batch_size:
            cursor.executemany(
                f"INSERT INTO {table} (file_path, obs_time) VALUES (%s, %s)",
                batch
            )
            connection.commit()
            print(f"[{file_type}] Committed batch of {len(batch)} at item {i+1} / {len(file_list)}")
            batch.clear()

    # Final batch
    if batch:
        cursor.executemany(
            f"INSERT INTO {table} (file_path, obs_time) VALUES (%s, %s)",
            batch
        )
        connection.commit()
        print(f"[{file_type}] Final batch committed with {len(batch)} entries")

    cursor.close()
    connection.close()
    print(f"[{file_type}] Total Inserted: {inserted}, Skipped: {skipped}")

# ##=========================
# # Get files list
# spec, slow_lev1, slow_lev15 = get_path_lwa_files(['2025-04-01T00:00:00', '2025-05-01T00:00:00'])
# print(f"Spec FITS: {len(spec)} found")
# print(f"Slow_lev1 HDF: {len(slow_lev1)} found")
# print(f"Slow_lev15 HDF: {len(slow_lev15)} found")

# # # # Insert to MySQL
# insert_file_list_to_mysql(spec, 'spec_fits')
# insert_file_list_to_mysql(slow_lev1, 'slow_lev1')
# insert_file_list_to_mysql(slow_lev15, 'slow_lev15')


# ##=========================
def delete_files_from_mysql(timerange):
    """
    Deletes entries in MySQL tables for slow_lev1, slow_lev15, and spec_fits files
    that fall within the specified time range.

    Parameters:
        timerange (list): A list of two ISO-formatted strings ['start_time', 'end_time'],
                          e.g., ['2024-12-28T00:00:00', '2025-01-05T00:00:00']
    """
    start = datetime.strptime(timerange[0], "%Y-%m-%dT%H:%M:%S")
    end = datetime.strptime(timerange[1], "%Y-%m-%dT%H:%M:%S")

    table_map = {
        'slow_lev1': 'lwa_slow_lev1_hdf_files',
        'slow_lev15': 'lwa_slow_lev15_hdf_files',
        'spec_fits': 'lwa_spec_fits_files'
    }

    connection = create_lwa_query_db_connection()
    cursor = connection.cursor()

    for key, table in table_map.items():
        cursor.execute(
            f"DELETE FROM {table} WHERE obs_time BETWEEN %s AND %s",
            (start, end)
        )
        print(f"[{key}] Deleted {cursor.rowcount} entries between {start} and {end}")

    connection.commit()
    cursor.close()
    connection.close()

# delete_files_from_mysql(['2024-12-20T00:00:00', '2025-01-15T00:00:00'])



##=========================
def main():
    parser = argparse.ArgumentParser(description="Insert or delete LWA metadata in MySQL")
    parser.add_argument('--start', required=True, help="Start time in format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument('--end', required=True, help="End time in format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument('--delete', action='store_true', help="If set, delete records instead of inserting")
    args = parser.parse_args()

    timerange = [args.start, args.end]

    if args.delete:
        delete_files_from_mysql(timerange)
    else:
        spec, slow_lev1, slow_lev15 = get_path_lwa_files(timerange)
        print(f"Spec FITS: {len(spec)} found")
        print(f"Slow_lev1 HDF: {len(slow_lev1)} found")
        print(f"Slow_lev15 HDF: {len(slow_lev15)} found")

        insert_file_list_to_mysql(spec, 'spec_fits')
        insert_file_list_to_mysql(slow_lev1, 'slow_lev1')
        insert_file_list_to_mysql(slow_lev15, 'slow_lev15')

if __name__ == '__main__':
    main()






##=========================
'''In MySQL : Old version

CREATE DATABASE lwa_metadata_query;

USE lwa_metadata_query;

CREATE TABLE lwa_fast_hdf_files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_path TEXT NOT NULL,
    obs_time DATETIME NOT NULL
);

CREATE TABLE lwa_slow_hdf_files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_path TEXT NOT NULL,
    obs_time DATETIME NOT NULL
);

CREATE TABLE lwa_spec_fits_files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_path TEXT NOT NULL,
    obs_time DATETIME NOT NULL
);


'''



# ##=========================
'''In MySQL : New version
'''

# ##=========================
# from datetime import datetime, timedelta
# '''Python Snippet to Generate the SQL'''
# def generate_partition_sql(start_year=2024, end_year=2030):
#     statements = []
#     current = datetime(start_year, 7, 1)

#     while current.year <= end_year:
#         next_month = current.replace(day=1) + timedelta(days=32)
#         next_month = next_month.replace(day=1)
#         label = current.strftime("p%Y%m")
#         date_str = next_month.strftime("%Y-%m-%d")
#         statements.append(f"PARTITION {label} VALUES LESS THAN (TO_DAYS('{date_str}'))")
#         current = next_month

#     statements.append("PARTITION pmax VALUES LESS THAN MAXVALUE")
#     return "ALTER TABLE lwa_slow_hdf_files ADD PARTITION (\n    " + ",\n    ".join(statements) + "\n);"

# print(generate_partition_sql())



# ##=========================
'''In MySQL : New version

CREATE DATABASE lwa_metadata_query;

USE lwa_metadata_query;

CREATE TABLE lwa_spec_fits_files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_path TEXT NOT NULL,
    obs_time DATETIME NOT NULL
);

CREATE INDEX idx_obs_time ON lwa_spec_fits_files (obs_time);


CREATE TABLE lwa_slow_lev1_hdf_files (
    id INT NOT NULL AUTO_INCREMENT,
    file_path TEXT NOT NULL,
    obs_time DATETIME NOT NULL,
    PRIMARY KEY (id, obs_time)
)
PARTITION BY RANGE (TO_DAYS(obs_time)) (
    PARTITION p202401 VALUES LESS THAN (TO_DAYS('2024-02-01')),
    PARTITION p202402 VALUES LESS THAN (TO_DAYS('2024-03-01')),
    PARTITION p202403 VALUES LESS THAN (TO_DAYS('2024-04-01')),
    PARTITION p202404 VALUES LESS THAN (TO_DAYS('2024-05-01')),
    PARTITION p202405 VALUES LESS THAN (TO_DAYS('2024-06-01')),
    PARTITION p202406 VALUES LESS THAN (TO_DAYS('2024-07-01')),
    PARTITION p202407 VALUES LESS THAN (TO_DAYS('2024-08-01')),
    PARTITION p202408 VALUES LESS THAN (TO_DAYS('2024-09-01')),
    PARTITION p202409 VALUES LESS THAN (TO_DAYS('2024-10-01')),
    PARTITION p202410 VALUES LESS THAN (TO_DAYS('2024-11-01')),
    PARTITION p202411 VALUES LESS THAN (TO_DAYS('2024-12-01')),
    PARTITION p202412 VALUES LESS THAN (TO_DAYS('2025-01-01')),
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION p202503 VALUES LESS THAN (TO_DAYS('2025-04-01')),
    PARTITION p202504 VALUES LESS THAN (TO_DAYS('2025-05-01')),
    PARTITION p202505 VALUES LESS THAN (TO_DAYS('2025-06-01')),
    PARTITION p202506 VALUES LESS THAN (TO_DAYS('2025-07-01')),
    PARTITION p202507 VALUES LESS THAN (TO_DAYS('2025-08-01')),
    PARTITION p202508 VALUES LESS THAN (TO_DAYS('2025-09-01')),
    PARTITION p202509 VALUES LESS THAN (TO_DAYS('2025-10-01')),
    PARTITION p202510 VALUES LESS THAN (TO_DAYS('2025-11-01')),
    PARTITION p202511 VALUES LESS THAN (TO_DAYS('2025-12-01')),
    PARTITION p202512 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p202601 VALUES LESS THAN (TO_DAYS('2026-02-01')),
    PARTITION p202602 VALUES LESS THAN (TO_DAYS('2026-03-01')),
    PARTITION p202603 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p202604 VALUES LESS THAN (TO_DAYS('2026-05-01')),
    PARTITION p202605 VALUES LESS THAN (TO_DAYS('2026-06-01')),
    PARTITION p202606 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p202607 VALUES LESS THAN (TO_DAYS('2026-08-01')),
    PARTITION p202608 VALUES LESS THAN (TO_DAYS('2026-09-01')),
    PARTITION p202609 VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p202610 VALUES LESS THAN (TO_DAYS('2026-11-01')),
    PARTITION p202611 VALUES LESS THAN (TO_DAYS('2026-12-01')),
    PARTITION p202612 VALUES LESS THAN (TO_DAYS('2027-01-01')),
    PARTITION p202701 VALUES LESS THAN (TO_DAYS('2027-02-01')),
    PARTITION p202702 VALUES LESS THAN (TO_DAYS('2027-03-01')),
    PARTITION p202703 VALUES LESS THAN (TO_DAYS('2027-04-01')),
    PARTITION p202704 VALUES LESS THAN (TO_DAYS('2027-05-01')),
    PARTITION p202705 VALUES LESS THAN (TO_DAYS('2027-06-01')),
    PARTITION p202706 VALUES LESS THAN (TO_DAYS('2027-07-01')),
    PARTITION p202707 VALUES LESS THAN (TO_DAYS('2027-08-01')),
    PARTITION p202708 VALUES LESS THAN (TO_DAYS('2027-09-01')),
    PARTITION p202709 VALUES LESS THAN (TO_DAYS('2027-10-01')),
    PARTITION p202710 VALUES LESS THAN (TO_DAYS('2027-11-01')),
    PARTITION p202711 VALUES LESS THAN (TO_DAYS('2027-12-01')),
    PARTITION p202712 VALUES LESS THAN (TO_DAYS('2028-01-01')),
    PARTITION p202801 VALUES LESS THAN (TO_DAYS('2028-02-01')),
    PARTITION p202802 VALUES LESS THAN (TO_DAYS('2028-03-01')),
    PARTITION p202803 VALUES LESS THAN (TO_DAYS('2028-04-01')),
    PARTITION p202804 VALUES LESS THAN (TO_DAYS('2028-05-01')),
    PARTITION p202805 VALUES LESS THAN (TO_DAYS('2028-06-01')),
    PARTITION p202806 VALUES LESS THAN (TO_DAYS('2028-07-01')),
    PARTITION p202807 VALUES LESS THAN (TO_DAYS('2028-08-01')),
    PARTITION p202808 VALUES LESS THAN (TO_DAYS('2028-09-01')),
    PARTITION p202809 VALUES LESS THAN (TO_DAYS('2028-10-01')),
    PARTITION p202810 VALUES LESS THAN (TO_DAYS('2028-11-01')),
    PARTITION p202811 VALUES LESS THAN (TO_DAYS('2028-12-01')),
    PARTITION p202812 VALUES LESS THAN (TO_DAYS('2029-01-01')),
    PARTITION p202901 VALUES LESS THAN (TO_DAYS('2029-02-01')),
    PARTITION p202902 VALUES LESS THAN (TO_DAYS('2029-03-01')),
    PARTITION p202903 VALUES LESS THAN (TO_DAYS('2029-04-01')),
    PARTITION p202904 VALUES LESS THAN (TO_DAYS('2029-05-01')),
    PARTITION p202905 VALUES LESS THAN (TO_DAYS('2029-06-01')),
    PARTITION p202906 VALUES LESS THAN (TO_DAYS('2029-07-01')),
    PARTITION p202907 VALUES LESS THAN (TO_DAYS('2029-08-01')),
    PARTITION p202908 VALUES LESS THAN (TO_DAYS('2029-09-01')),
    PARTITION p202909 VALUES LESS THAN (TO_DAYS('2029-10-01')),
    PARTITION p202910 VALUES LESS THAN (TO_DAYS('2029-11-01')),
    PARTITION p202911 VALUES LESS THAN (TO_DAYS('2029-12-01')),
    PARTITION p202912 VALUES LESS THAN (TO_DAYS('2030-01-01')),
    PARTITION p203001 VALUES LESS THAN (TO_DAYS('2030-02-01')),
    PARTITION p203002 VALUES LESS THAN (TO_DAYS('2030-03-01')),
    PARTITION p203003 VALUES LESS THAN (TO_DAYS('2030-04-01')),
    PARTITION p203004 VALUES LESS THAN (TO_DAYS('2030-05-01')),
    PARTITION p203005 VALUES LESS THAN (TO_DAYS('2030-06-01')),
    PARTITION p203006 VALUES LESS THAN (TO_DAYS('2030-07-01')),
    PARTITION p203007 VALUES LESS THAN (TO_DAYS('2030-08-01')),
    PARTITION p203008 VALUES LESS THAN (TO_DAYS('2030-09-01')),
    PARTITION p203009 VALUES LESS THAN (TO_DAYS('2030-10-01')),
    PARTITION p203010 VALUES LESS THAN (TO_DAYS('2030-11-01')),
    PARTITION p203011 VALUES LESS THAN (TO_DAYS('2030-12-01')),
    PARTITION p203012 VALUES LESS THAN (TO_DAYS('2031-01-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);




CREATE TABLE lwa_slow_lev15_hdf_files (
    id INT NOT NULL AUTO_INCREMENT,
    file_path TEXT NOT NULL,
    obs_time DATETIME NOT NULL,
    PRIMARY KEY (id, obs_time)
)
PARTITION BY RANGE (TO_DAYS(obs_time)) (
    PARTITION p202401 VALUES LESS THAN (TO_DAYS('2024-02-01')),
    PARTITION p202402 VALUES LESS THAN (TO_DAYS('2024-03-01')),
    PARTITION p202403 VALUES LESS THAN (TO_DAYS('2024-04-01')),
    PARTITION p202404 VALUES LESS THAN (TO_DAYS('2024-05-01')),
    PARTITION p202405 VALUES LESS THAN (TO_DAYS('2024-06-01')),
    PARTITION p202406 VALUES LESS THAN (TO_DAYS('2024-07-01')),
    PARTITION p202407 VALUES LESS THAN (TO_DAYS('2024-08-01')),
    PARTITION p202408 VALUES LESS THAN (TO_DAYS('2024-09-01')),
    PARTITION p202409 VALUES LESS THAN (TO_DAYS('2024-10-01')),
    PARTITION p202410 VALUES LESS THAN (TO_DAYS('2024-11-01')),
    PARTITION p202411 VALUES LESS THAN (TO_DAYS('2024-12-01')),
    PARTITION p202412 VALUES LESS THAN (TO_DAYS('2025-01-01')),
    PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION p202503 VALUES LESS THAN (TO_DAYS('2025-04-01')),
    PARTITION p202504 VALUES LESS THAN (TO_DAYS('2025-05-01')),
    PARTITION p202505 VALUES LESS THAN (TO_DAYS('2025-06-01')),
    PARTITION p202506 VALUES LESS THAN (TO_DAYS('2025-07-01')),
    PARTITION p202507 VALUES LESS THAN (TO_DAYS('2025-08-01')),
    PARTITION p202508 VALUES LESS THAN (TO_DAYS('2025-09-01')),
    PARTITION p202509 VALUES LESS THAN (TO_DAYS('2025-10-01')),
    PARTITION p202510 VALUES LESS THAN (TO_DAYS('2025-11-01')),
    PARTITION p202511 VALUES LESS THAN (TO_DAYS('2025-12-01')),
    PARTITION p202512 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p202601 VALUES LESS THAN (TO_DAYS('2026-02-01')),
    PARTITION p202602 VALUES LESS THAN (TO_DAYS('2026-03-01')),
    PARTITION p202603 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p202604 VALUES LESS THAN (TO_DAYS('2026-05-01')),
    PARTITION p202605 VALUES LESS THAN (TO_DAYS('2026-06-01')),
    PARTITION p202606 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p202607 VALUES LESS THAN (TO_DAYS('2026-08-01')),
    PARTITION p202608 VALUES LESS THAN (TO_DAYS('2026-09-01')),
    PARTITION p202609 VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p202610 VALUES LESS THAN (TO_DAYS('2026-11-01')),
    PARTITION p202611 VALUES LESS THAN (TO_DAYS('2026-12-01')),
    PARTITION p202612 VALUES LESS THAN (TO_DAYS('2027-01-01')),
    PARTITION p202701 VALUES LESS THAN (TO_DAYS('2027-02-01')),
    PARTITION p202702 VALUES LESS THAN (TO_DAYS('2027-03-01')),
    PARTITION p202703 VALUES LESS THAN (TO_DAYS('2027-04-01')),
    PARTITION p202704 VALUES LESS THAN (TO_DAYS('2027-05-01')),
    PARTITION p202705 VALUES LESS THAN (TO_DAYS('2027-06-01')),
    PARTITION p202706 VALUES LESS THAN (TO_DAYS('2027-07-01')),
    PARTITION p202707 VALUES LESS THAN (TO_DAYS('2027-08-01')),
    PARTITION p202708 VALUES LESS THAN (TO_DAYS('2027-09-01')),
    PARTITION p202709 VALUES LESS THAN (TO_DAYS('2027-10-01')),
    PARTITION p202710 VALUES LESS THAN (TO_DAYS('2027-11-01')),
    PARTITION p202711 VALUES LESS THAN (TO_DAYS('2027-12-01')),
    PARTITION p202712 VALUES LESS THAN (TO_DAYS('2028-01-01')),
    PARTITION p202801 VALUES LESS THAN (TO_DAYS('2028-02-01')),
    PARTITION p202802 VALUES LESS THAN (TO_DAYS('2028-03-01')),
    PARTITION p202803 VALUES LESS THAN (TO_DAYS('2028-04-01')),
    PARTITION p202804 VALUES LESS THAN (TO_DAYS('2028-05-01')),
    PARTITION p202805 VALUES LESS THAN (TO_DAYS('2028-06-01')),
    PARTITION p202806 VALUES LESS THAN (TO_DAYS('2028-07-01')),
    PARTITION p202807 VALUES LESS THAN (TO_DAYS('2028-08-01')),
    PARTITION p202808 VALUES LESS THAN (TO_DAYS('2028-09-01')),
    PARTITION p202809 VALUES LESS THAN (TO_DAYS('2028-10-01')),
    PARTITION p202810 VALUES LESS THAN (TO_DAYS('2028-11-01')),
    PARTITION p202811 VALUES LESS THAN (TO_DAYS('2028-12-01')),
    PARTITION p202812 VALUES LESS THAN (TO_DAYS('2029-01-01')),
    PARTITION p202901 VALUES LESS THAN (TO_DAYS('2029-02-01')),
    PARTITION p202902 VALUES LESS THAN (TO_DAYS('2029-03-01')),
    PARTITION p202903 VALUES LESS THAN (TO_DAYS('2029-04-01')),
    PARTITION p202904 VALUES LESS THAN (TO_DAYS('2029-05-01')),
    PARTITION p202905 VALUES LESS THAN (TO_DAYS('2029-06-01')),
    PARTITION p202906 VALUES LESS THAN (TO_DAYS('2029-07-01')),
    PARTITION p202907 VALUES LESS THAN (TO_DAYS('2029-08-01')),
    PARTITION p202908 VALUES LESS THAN (TO_DAYS('2029-09-01')),
    PARTITION p202909 VALUES LESS THAN (TO_DAYS('2029-10-01')),
    PARTITION p202910 VALUES LESS THAN (TO_DAYS('2029-11-01')),
    PARTITION p202911 VALUES LESS THAN (TO_DAYS('2029-12-01')),
    PARTITION p202912 VALUES LESS THAN (TO_DAYS('2030-01-01')),
    PARTITION p203001 VALUES LESS THAN (TO_DAYS('2030-02-01')),
    PARTITION p203002 VALUES LESS THAN (TO_DAYS('2030-03-01')),
    PARTITION p203003 VALUES LESS THAN (TO_DAYS('2030-04-01')),
    PARTITION p203004 VALUES LESS THAN (TO_DAYS('2030-05-01')),
    PARTITION p203005 VALUES LESS THAN (TO_DAYS('2030-06-01')),
    PARTITION p203006 VALUES LESS THAN (TO_DAYS('2030-07-01')),
    PARTITION p203007 VALUES LESS THAN (TO_DAYS('2030-08-01')),
    PARTITION p203008 VALUES LESS THAN (TO_DAYS('2030-09-01')),
    PARTITION p203009 VALUES LESS THAN (TO_DAYS('2030-10-01')),
    PARTITION p203010 VALUES LESS THAN (TO_DAYS('2030-11-01')),
    PARTITION p203011 VALUES LESS THAN (TO_DAYS('2030-12-01')),
    PARTITION p203012 VALUES LESS THAN (TO_DAYS('2031-01-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



'''

# ##=========================
'''
-- To list COLUMNS

SHOW COLUMNS FROM lwa_slow_lev1_hdf_files;

-- To list all partitions
SELECT
    TABLE_NAME,
    PARTITION_NAME,
    PARTITION_DESCRIPTION,
    TABLE_ROWS
FROM
    information_schema.PARTITIONS
WHERE
    TABLE_SCHEMA = 'lwa_metadata_query'
    AND TABLE_NAME IN ('lwa_slow_lev1_hdf_files', 'lwa_slow_lev15_hdf_files')
ORDER BY
    TABLE_NAME, PARTITION_DESCRIPTION;

'''


# ##=========================
'''
-- Add more partitions
-- Drop existing MAXVALUE partition
ALTER TABLE lwa_slow_lev1_hdf_files
DROP PARTITION pmax;

-- Add monthly partitions
ALTER TABLE lwa_slow_lev1_hdf_files
ADD PARTITION (
    PARTITION p203101 VALUES LESS THAN (TO_DAYS('2031-02-01')),
    PARTITION p203102 VALUES LESS THAN (TO_DAYS('2031-03-01')),
    PARTITION p203103 VALUES LESS THAN (TO_DAYS('2031-04-01')),
    , PARTITION pmax VALUES LESS THAN MAXVALUE
);

'''

# def generate_partition_sql(start_year=2031, end_year=2032, drop_pmax=True, readd_pmax=True):
#     sql_lines = []

#     if drop_pmax:
#         sql_lines.append("-- Drop existing MAXVALUE partition")
#         sql_lines.append("ALTER TABLE lwa_slow_lev1_hdf_files DROP PARTITION pmax;")
#         sql_lines.append("")

#     sql_lines.append("-- Add monthly partitions")
#     sql_lines.append("ALTER TABLE lwa_slow_lev1_hdf_files ADD PARTITION (")

#     parts = []
#     for year in range(start_year, end_year + 1):
#         for month in range(1, 13):
#             month_str = f"{month:02d}"
#             next_month = f"{month + 1:02d}"
#             next_year = year
#             if month == 12:
#                 next_month = "01"
#                 next_year = year + 1
#             label = f"p{year}{month_str}"
#             cutoff = f"{next_year}-{next_month}-01"
#             parts.append(f"    PARTITION {label} VALUES LESS THAN (TO_DAYS('{cutoff}'))")

#     sql_lines.append(",\n".join(parts))

#     if readd_pmax:
#         sql_lines.append("    , PARTITION pmax VALUES LESS THAN MAXVALUE")

#     sql_lines.append(");")
#     return "\n".join(sql_lines)

# # # Example usage
# # if __name__ == "__main__":
# print(generate_partition_sql(start_year=2031, end_year=2033))









