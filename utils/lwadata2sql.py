## lwadata2sql.py
## python lwadata2sql.py --start 2025-04-01T00:00:00 --end 2025-05-01T00:00:00
## python lwadata2sql.py --start 2025-04-30T00:00:00 --end 2025-05-01T00:00:00 --delete

import mysql.connector
import os
from glob import glob
from datetime import datetime, timedelta
import argparse
import re
from astropy.io import fits

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
    """
    Parameters:
        file_type: one of ["spec", "mfs_lev1", "mfs_lev15", "fch_lev1", "fch_lev15"]
        spec: ovro-lwa.lev1_bmf_256ms_96kHz.YYYY-MM-DD.dspec_I.fits
        image: ovro-lwa-352.lev1_mfs_10s.YYYY-MM-DDThhmmssZ.image_I.hdf
    Return:
        spec: start_time, end_time
        image: obs_time
    """
    try:
        if file_type == 'spec':
            with fits.open(filepath) as hdul:
                header = hdul[0].header
                start_str = header.get("DATE_OBS")
                end_str = header.get("DATE_END")
                if not start_str or not end_str:
                    return None, None
                start_time = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%S.%f")
                end_time = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%S.%f")
                return start_time, end_time
        elif file_type.startswith(('mfs_', 'fch_')):
            filename = os.path.basename(filepath)
            # Use regex to extract ISO-like timestamp
            match = re.search(r'\d{4}-\d{2}-\d{2}T\d{6}Z', filename)
            if match:
                return datetime.strptime(match.group(0), "%Y-%m-%dT%H%M%SZ")
    except Exception:
        return None

def filter_and_log(files, file_type, timerange):
    '''Filter a list of files within a given timerange.
    Return:
        a list of (file_path, start_time, end_time) tuples for spec files.
        a list of (file_path, obs_time) tuples for image files, 
    '''
    print(f"file_type: {file_type}")
    start_range = datetime.strptime(timerange[0], "%Y-%m-%dT%H:%M:%S")
    end_range = datetime.strptime(timerange[1], "%Y-%m-%dT%H:%M:%S")
    result = []
    last_day = None

    if file_type == 'spec':
        for f in sorted(files):
            # First filter based on filename date
            filename = os.path.basename(f)
            try:
                match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
                if match:
                    file_date = datetime.strptime(match.group(0), "%Y-%m-%d")
            except Exception:
                continue
            if file_date < (start_range - timedelta(days=1)) or file_date > (end_range + timedelta(days=1)):
                continue
            try:
                st, ed = parse_obs_time(f, file_type)
                if ed >= start_range and st <= end_range:
                    result.append((f, st, ed))
            except Exception as e:
                print(f"Warning: Failed to read header for {f} -- {e}")
        return result

    elif file_type.startswith(('mfs_', 'fch_')):
        for f in sorted(files):
            try:
                t = parse_obs_time(f, file_type)
                if not t or not (start_range <= t <= end_range):
                    continue
                current_day = t.strftime("%Y-%m-%d")
                if current_day != last_day:
                    print(current_day)
                    last_day = current_day
                result.append((f, t))
            except Exception as e:
                print(f"Skipped {f}: {e}")
        return result
    else:
        raise ValueError(f"Unsupported file_type: {file_type}")

def get_path_lwa_files(timerange, file_type="spec"):
    """
    Parameters:
        timerange: [start_str, end_str] in ISO format e.g. "2024-01-01T00:00:00"
        file_type: one of ["spec", "mfs_lev1", "mfs_lev15", "fch_lev1", "fch_lev15"]
    Returns:
        List of file paths matching the type and time range
    History:
        2025-04-08, initial version supporting "spec", "slow_lev1", "slow_lev15"
        2025-06-08, added support for "fch_lev1" and "fch_lev15"
        2025-06-17, new path/name for spec fits data
    """
    start = datetime.strptime(timerange[0], "%Y-%m-%dT%H:%M:%S")
    end = datetime.strptime(timerange[1], "%Y-%m-%dT%H:%M:%S")
    start_1daybf = start - timedelta(days=1)
    end_1dayaf = end + timedelta(days=1)

    files_collected = []
    if file_type == "spec":
        for year in range(start_1daybf.year, end_1dayaf.year+1):
            pattern = f"/common/lwa/spec/fits/{year}/*.fits"
            files_collected += glob(pattern)
    else:
        # mfs/fch lev1/lev15 HDF files
        image_type, level = file_type.split("_")  # e.g., mfs, lev1
        level_dir = "lev1" if level == "lev1" else "lev15"
        pattern = f"*_{image_type}_*.hdf"
        date_cursor = start_1daybf
        while date_cursor <= end_1dayaf:
            y, m, d = date_cursor.strftime("%Y"), date_cursor.strftime("%m"), date_cursor.strftime("%d")
            for disk in ['nas7']:#'nas6', 
                files_collected += glob(f"/{disk}/ovro-lwa-data/hdf/slow/{level_dir}/{y}/{m}/{d}/{pattern}")
            date_cursor += timedelta(days=1)
    # Filter and return sorted paths
    files_filtered = filter_and_log(files_collected, file_type, timerange)
    files_sorted = [f for f, *_ in files_filtered]
    return files_sorted

# file_types = ["spec", "mfs_lev1", "mfs_lev15", "fch_lev1", "fch_lev15"]
# for file_type in file_types:
#     files = get_path_lwa_files(['2025-04-01T00:00:00', '2025-05-01T00:00:00'], file_type=file_type)
#     print(f"{file_type}: {len(files)} found")

##=========================
def insert_file_list_to_mysql(file_list, file_type, batch_size=1000):
    # # table_map = {
    # #     'fast_hdf': 'lwa_fast_hdf_files',
    # #     'slow_hdf': 'lwa_slow_hdf_files',
    # #     'spec_fits': 'lwa_spec_fits_files'
    # # }
    # table_map = {
    #     'slow_lev1': 'lwa_slow_lev1_hdf_files',
    #     'slow_lev15': 'lwa_slow_lev15_hdf_files',
    #     'spec_fits': 'lwa_spec_fits_files'
    # }
    table_map = {
        'spec':        'lwa_spec_fits_files',
        'mfs_lev1':    'lwa_slow_mfs_lev1_hdf_files',
        'mfs_lev15':   'lwa_slow_mfs_lev15_hdf_files',
        'fch_lev1':    'lwa_slow_fch_lev1_hdf_files',
        'fch_lev15':   'lwa_slow_fch_lev15_hdf_files'
    }

    if file_type not in table_map:
        raise ValueError(f"Unsupported file_type: {file_type}")

    table = table_map[file_type]
    batch = []
    inserted_total = 0
    connection = create_lwa_query_db_connection()
    cursor = connection.cursor()

    for i, file_path in enumerate(file_list):
        if file_type == 'spec':
            start_time, end_time = parse_obs_time(file_path, file_type)
            if not start_time or not end_time:
                continue
            batch.append((file_path, start_time, end_time))

            if len(batch) >= batch_size:
                cursor.executemany(
                    f"INSERT IGNORE INTO {table} (file_path, start_time, end_time) VALUES (%s, %s, %s)",
                    batch
                )
                inserted_total += cursor.rowcount
                connection.commit()
                print(f"[{file_type}] Committed batch of {len(batch)} at item {i+1} / {len(file_list)}")
                batch.clear()

        else:
            obs_time = parse_obs_time(file_path, file_type)
            if not obs_time:
                continue
            batch.append((file_path, obs_time))

            if len(batch) >= batch_size:
                cursor.executemany(
                    f"INSERT IGNORE INTO {table} (file_path, obs_time) VALUES (%s, %s)",
                    batch
                )
                inserted_total += cursor.rowcount
                connection.commit()
                print(f"[{file_type}] Committed batch of {len(batch)} at item {i+1} / {len(file_list)}")
                batch.clear()

    if batch:
        if file_type == 'spec':
            cursor.executemany(
                f"INSERT IGNORE INTO {table} (file_path, start_time, end_time) VALUES (%s, %s, %s)",
                batch
            )
        else:
            cursor.executemany(
                f"INSERT IGNORE INTO {table} (file_path, obs_time) VALUES (%s, %s)",
                batch
            )
        inserted_total += cursor.rowcount
        connection.commit()
        print(f"[{file_type}] Final batch committed with {len(batch)} entries")

    cursor.close()
    connection.close()
    print(f"[{file_type}] Total Inserted: {inserted_total}, Skipped: {len(file_list) - inserted_total}")

# # ##=========================
# file_types = ["spec", "mfs_lev1", "mfs_lev15", "fch_lev1", "fch_lev15"]
# for file_type in file_types:
#     # Get files list
#     files = get_path_lwa_files(['2025-04-01T00:00:00', '2025-04-03T00:00:00'], file_type=file_type)
#     print(f"{file_type}: {len(files)} found")
#     # Insert to MySQL
#     insert_file_list_to_mysql(files, file_type)


##=========================
def delete_files_from_mysql(timerange, file_type=None):
    """
    Deletes entries in MySQL tables for spec, mfs/fch lev1/lev15 files, or all file types if not specified
    that fall within the specified time range.
    timerange is in ISO format (e.g., ['2024-12-28T00:00:00', '2025-01-05T00:00:00'])
    """
    start = datetime.strptime(timerange[0], "%Y-%m-%dT%H:%M:%S")
    end = datetime.strptime(timerange[1], "%Y-%m-%dT%H:%M:%S")

    table_map = {
        'spec':        ('lwa_spec_fits_files', 'end_time'),
        'mfs_lev1':    ('lwa_slow_mfs_lev1_hdf_files', 'obs_time'),
        'mfs_lev15':   ('lwa_slow_mfs_lev15_hdf_files', 'obs_time'),
        'fch_lev1':    ('lwa_slow_fch_lev1_hdf_files', 'obs_time'),
        'fch_lev15':   ('lwa_slow_fch_lev15_hdf_files', 'obs_time')
    }

    connection = create_lwa_query_db_connection()
    cursor = connection.cursor()

    targets = [file_type] if file_type else table_map.keys()

    for key in targets:
        if key not in table_map:
            print(f"[{key}] Skipped: unsupported file_type")
            continue

        table, time_column = table_map[key]
        cursor.execute(
            f"DELETE FROM {table} WHERE {time_column} BETWEEN %s AND %s",
            (start, end)
        )
        print(f"[{key}] Deleted {cursor.rowcount} entries between {start} and {end}")

    connection.commit()
    cursor.close()
    connection.close()

# delete_files_from_mysql(['2024-12-20T00:00:00', '2025-01-15T00:00:00']) ##will delete all files
# delete_files_from_mysql(['2024-12-20T00:00:00', '2025-01-15T00:00:00'], file_type="spec") ##will delete spec files


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
        file_types = ["spec", "mfs_lev1", "mfs_lev15", "fch_lev1", "fch_lev15"]
        for file_type in file_types:
            # Get files list
            files = get_path_lwa_files(timerange, file_type=file_type)
            print(f"{file_type}: {len(files)} found")
            # Insert to MySQL
            insert_file_list_to_mysql(files, file_type)
            print(f"Success for {file_type}!")


if __name__ == '__main__':
    main()








# ##=========================
'''In MySQL :
History:
    2025-04-08, initial version
    2025-05-02, add month index for quick search
    2025-06-08, add table for "lwa_slow_fch_lev1_hdf_files" and "lwa_slow_fch_lev15_hdf_files"
    2025-06-18, new table for spec
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
'''In MySQL : version - June-18

CREATE TABLE lwa_spec_fits_files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_path VARCHAR(1024) NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    UNIQUE KEY uq_file_path (file_path(255)),
    INDEX idx_start_time (start_time),
    INDEX idx_end_time (end_time)
);

'''



# ##=========================
'''In MySQL : version - June-07

CREATE DATABASE lwa_metadata_query;

USE lwa_metadata_query;

CREATE TABLE lwa_spec_fits_files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_path VARCHAR(1024) NOT NULL,
    obs_time DATETIME NOT NULL,
    UNIQUE KEY uq_file_path (file_path(255)),
    INDEX idx_obs_time (obs_time)
);


CREATE TABLE lwa_slow_fch_lev15_hdf_files (
    id INT NOT NULL AUTO_INCREMENT,
    file_path VARCHAR(1024) NOT NULL,
    obs_time DATETIME NOT NULL,
    UNIQUE KEY uq_file_path (file_path(255), obs_time),
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


PARTITION BY RANGE (TO_DAYS(obs_time)) (
    PARTITION p202401 VALUES LESS THAN (TO_DAYS('2024-02-01')),
    PARTITION p202402 VALUES LESS THAN (TO_DAYS('2024-03-01')),
    ...
    PARTITION p203012 VALUES LESS THAN (TO_DAYS('2031-01-01')),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);


'''


# ##=========================
'''In MySQL : version - May-01

CREATE DATABASE lwa_metadata_query;

USE lwa_metadata_query;

CREATE TABLE lwa_spec_fits_files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_path TEXT NOT NULL,
    obs_time DATETIME NOT NULL
);

CREATE INDEX idx_obs_time ON lwa_spec_fits_files (obs_time);


CREATE TABLE lwa_slow_fch_lev1_hdf_files (
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


##=========================
'''In MySQL : Old version - before Apr

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
'''
-- To list COLUMNS

SHOW COLUMNS FROM lwa_slow_mfs_lev1_hdf_files;

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
    AND TABLE_NAME IN ('lwa_slow_mfs_lev1_hdf_files', 'lwa_slow_mfs_lev15_hdf_files')
ORDER BY
    TABLE_NAME, PARTITION_DESCRIPTION;

'''


# ##=========================
'''
-- Add more partitions
-- Drop existing MAXVALUE partition
ALTER TABLE lwa_slow_mfs_lev1_hdf_files
DROP PARTITION pmax;

-- Add monthly partitions
ALTER TABLE lwa_slow_mfs_lev1_hdf_files
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
#         sql_lines.append("ALTER TABLE lwa_slow_mfs_lev1_hdf_files DROP PARTITION pmax;")
#         sql_lines.append("")

#     sql_lines.append("-- Add monthly partitions")
#     sql_lines.append("ALTER TABLE lwa_slow_mfs_lev1_hdf_files ADD PARTITION (")

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

