##python lwa-query-web_utils.py --start 2025-04-25 --end 2025-05-01
##python lwa-query-web_utils.py --gen movie --start 2025-04-25 --end 2025-05-01
##python lwa-query-web_utils.py --start 2025-04-25 --end 2025-05-01 --out /tmp/movies
##du -h /common/webplots/lwa-data/qlook_daily/movies/*.mp4
import mysql.connector
import os
from glob import glob
from datetime import datetime, timedelta
import subprocess
import tempfile
import shutil
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

##=========================connect to database
def get_lwa_file_lists_from_mysql(start_utc, end_utc, image_type="mfs"):
    start = Time(start_utc).datetime
    end = Time(end_utc).datetime
    # Choose table based on image_type
    if image_type == "mfs":
        tables = {
            'spec_fits': 'lwa_spec_fits_files',
            'slow_lev1': 'lwa_slow_mfs_lev1_hdf_files',
            'slow_lev15': 'lwa_slow_mfs_lev15_hdf_files'
        }
    elif image_type == "fch":
        tables = {
            'spec_fits': 'lwa_spec_fits_files',
            'slow_lev1': 'lwa_slow_fch_lev1_hdf_files',
            'slow_lev15': 'lwa_slow_fch_lev15_hdf_files'
        }
    else:
        raise ValueError(f"Unsupported image_type: {image_type}")
    # Connection
    connection = create_lwa_query_db_connection()
    cursor = connection.cursor()
    query = """
        SELECT file_path, obs_time FROM {table}
        WHERE obs_time BETWEEN %s AND %s
        ORDER BY obs_time
    """
    file_lists = {}
    obs_times = {}
    for file_type, table in tables.items():
        cursor.execute(query.format(table=table), (start, end))
        rows = cursor.fetchall()
        file_lists[file_type] = [row[0] for row in rows]
        obs_times[file_type] = [row[1] for row in rows]
    cursor.close()
    connection.close()
    return file_lists, obs_times

# Example usage:
# file_lists, obs_times = get_lwa_file_lists_from_mysql("2025-04-01T00:00:00", "2025-05-01T00:00:00", image_type="mfs_lev15")


##=========================
def generate_movies_from_date_range(start_date_str, end_date_str, save_path='/common/webplots/lwa-data/qlook_daily/movies/'):
    """
    Given a date range in 'YYYY-MM-DD' format, find PNGs under the
    corresponding LWA synoptic image directory and generate a movie per day,
    restricted to 12:00 of the day to 03:00 of the next day.

    Args:
        start_date_str (str): Start date in 'YYYY-MM-DD'
        end_date_str (str): End date in 'YYYY-MM-DD'
        save_path (str): Directory where movies are saved

    Returns:
        dict: {date_str: movie_relative_path or None}
    """
    results = {}
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        current_date = start_date

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            yyyy, mm, dd = current_date.strftime("%Y"), current_date.strftime("%m"), current_date.strftime("%d")
            img_dir = f"/common/webplots/lwa-data/qlook_images/slow/synop/{yyyy}/{mm}/{dd}"
            all_png_files = sorted(glob(os.path.join(img_dir, "*.png")))

            if not all_png_files:
                print(f"[{date_str}] No PNG files found.")
                results[date_str] = None
                current_date += timedelta(days=1)
                continue

            start_time = datetime.combine(current_date, datetime.strptime("12:00:00", "%H:%M:%S").time())
            end_time = datetime.combine(current_date + timedelta(days=1), datetime.strptime("03:00:00", "%H:%M:%S").time())

            def extract_timestamp(path):
                base = os.path.basename(path)
                try:
                    tstr = base.split("T")[1][:6]  # 'HHMMSS'
                    dstr = base.split("T")[0].split(".")[-1]  # 'YYYY-MM-DD'
                    return datetime.strptime(dstr + tstr, "%Y-%m-%d%H%M%S")
                except Exception:
                    return None

            png_files = [f for f in all_png_files if (ts := extract_timestamp(f)) and start_time <= ts <= end_time]
            if not png_files:
                print(f"[{date_str}] No PNGs in 12:00-03:00 window.")
                results[date_str] = None
                current_date += timedelta(days=1)
                continue

            print(f"[{date_str}] {len(png_files)} PNGs in time window.")
            temp_dir = tempfile.mkdtemp()

            try:
                # It creates temporary symbolic links to those files in a temp directory:                
                for i, f in enumerate(png_files):
                    os.symlink(f, os.path.join(temp_dir, f"{i:04d}.png"))

                output_name = f"slow_hdf_movie_{yyyy}{mm}{dd}.mp4"
                output_path = os.path.join(save_path, output_name)
                print("path: ", os.path.dirname(output_path))
                os.makedirs(os.path.dirname(output_path), exist_ok=True)

                cmd = [
                    "ffmpeg", "-y",
                    "-framerate", "6",
                    "-i", os.path.join(temp_dir, "%04d.png"),
                    "-c:v", "libx264", "-pix_fmt", "yuv420p",
                    output_path
                ]
                subprocess.run(cmd, check=True)
                results[date_str] = f"{save_path}{output_name}"

            except Exception as e:
                print(f"[{date_str}] Movie generation failed: {e}")
                results[date_str] = None
            finally:
                # After the movie is generated, it cleans up the temporary symlinks only:
                shutil.rmtree(temp_dir)

            current_date += timedelta(days=1)

    except Exception as e:
        print(f"[ERROR] Failed to process date range: {e}")

    return results

# Example usage:
# paths = generate_movies_from_date_range('2025-04-25', '2025-04-27')
# print(paths)


##=========================
def main():
    parser = argparse.ArgumentParser(description="LWA Query and Movie Generator Utility")
    parser.add_argument('--gen', choices=['movie'], help="Optional generation mode: e.g., 'movie'")
    parser.add_argument('--start', required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument('--end', required=True, help="End date in YYYY-MM-DD")
    parser.add_argument('--out', default='/common/webplots/lwa-data/qlook_daily/movies/', help="Output directory (default: /common/webplots/lwa-data/qlook_daily/movies/)")

    args = parser.parse_args()

    if args.gen == 'movie':
        print(f"Generating movies from {args.start} to {args.end}...")
        results = generate_movies_from_date_range(args.start, args.end, save_path=args.out)
        print("\nGenerated movie paths:")
        for date_str, path in results.items():
            print(f"{date_str}: {path if path else 'No movie generated'}")
    else:
        print(f"No generation mode specified. Proceeding with default action from {args.start} to {args.end}...")

if __name__ == '__main__':
    main()








