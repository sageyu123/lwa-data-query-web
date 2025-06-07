import numpy as np
import pandas as pd
import os
import mysql.connector
from flask import Flask, Blueprint, render_template, request, jsonify, url_for, redirect, send_file
import plotly
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import re
import socket
import json
import requests
from pathlib import Path
import subprocess
import tempfile
from astropy.time import Time
from datetime import datetime, timedelta
from glob import glob
import shutil
from imageio import imread
import logging

logger = logging.getLogger(__name__)

##=========================
example = Blueprint('example', __name__, template_folder='templates')

lwadata_dir = '/common/webplots/lwa-data'
data_subdir = 'tmp/data-request'
movie_subdir = 'tmp/html'

##=========================
max_IP_downloads_per_day = 20
max_MB_downloads_per_IP = 100.
lwa_user_downloads_log_path = "/home/xychen/lwadata-query-web-utils/lwa_user_downloads_log.json"

##=========================
def create_lwa_query_db_connection():
    return mysql.connector.connect(
        host=os.getenv('FLARE_DB_HOST'),
        database='lwa_metadata_query',
        user=os.getenv('FLARE_DB_USER'),
        password=os.getenv('FLARE_DB_PASSWORD')
    )

##=========================
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

##=========================
def convert_local_to_urls(files_path):
    """
    Convert local HDF paths to public HTTPS URLs.

    Parameters:
        files_path (list): List of local HDF file paths (str)

    Returns:
        list: List of converted HTTPS URLs
    """
    urls = []
    for file_path in files_path:
        if file_path.startswith("/nas7/ovro-lwa-data/hdf/"):
            url = file_path.replace("/nas7/ovro-lwa-data/hdf/", "https://ovsa.njit.edu/lwadata3/hdf/")
        elif file_path.startswith("/nas6/ovro-lwa-data/hdf/"):
            url = file_path.replace("/nas6/ovro-lwa-data/hdf/", "https://ovsa.njit.edu/lwadata2/hdf/")
        elif file_path.startswith("/common/lwa/spec_v2/fits/"):
            filename = os.path.basename(file_path)
            url = f"https://ovsa.njit.edu/lwa/extm/fits/{filename}"
        else:
            url = file_path  # fallback
        urls.append(url)
    return urls

##=========================
def convert_local_to_filename(files_path):
    """
    Convert local HDF paths to filename only.

    Parameters:
        files_path (list): List of local HDF file paths (str)

    Returns:
        list: List of converted file names
    """
    return [os.path.basename(path) for path in files_path]

##=========================
def convert_slow_hdf_to_existing_png(hdf_list):
    """
    Convert each .hdf path (lev1 or lev15) to its corresponding .png path by timestamp match,
    and return only those that actually exist on disk.

    Parameters:
        hdf_list (list): List of full paths to .hdf files

    Returns:
        List of existing .png file paths
    """
    png_list = []
    for hdf_path in hdf_list:
        try:
            hdf_filename = os.path.basename(hdf_path)
            # Extract timestamp part
            if "T" in hdf_filename and hdf_filename.endswith(".hdf"):
                timestamp_part = hdf_filename.split("T")[1].split("Z")[0]  # e.g., "123456"
                date_part = hdf_filename.split("T")[0].split('.')[-1]      # e.g., "2025-05-10"
                yyyy, mm, dd = date_part.split('-')
            else:
                continue  # skip if format unexpected
            # Reconstruct PNG filename
            # Example: ovro-lwa-352.synop_mfs_10s.2025-05-10T123456Z.image_I.png
            prefix = "ovro-lwa-352.synop_mfs_10s"
            png_filename = f"{prefix}.{date_part}T{timestamp_part}Z.image_I.png"
            # png_filename = hdf_filename.replace('.lev1.5_', '.synop_').replace('.hdf', '.png')
            png_path = f"{lwadata_dir}/qlook_images/slow/synop/{yyyy}/{mm}/{dd}/{png_filename}"
            if os.path.exists(png_path):
                png_list.append(png_path)
        except Exception as e:
            logger.warning("Error processing %s: %s", hdf_path, e)
            continue
    return png_list
# png_files = convert_slow_hdf_to_existing_png(slow_hdf_files)

##=========================
def convert_png_to_urls(png_paths):
    """
    Convert full PNG file paths to public HTTPS URLs based on path pattern.

    Parameters:
        png_paths (list): Full local PNG file paths (e.g., /common/webplots/.../png)

    Returns:
        List of HTTPS URLs pointing to each image.
    """
    urls = []
    for png_path in sorted(png_paths):
        try:
            png_filename = os.path.basename(png_path)

            # Extract date/time string for folder and filename
            timestamp_part = png_filename.split("T")[1].split("Z")[0]  # e.g., "123456"
            date_str = png_filename.split("T")[0].split(".")[-1]  # e.g., 2025-05-10
            yyyy, mm, dd = date_str.split("-")
            url = f"https://ovsa.njit.edu/lwa-data/qlook_images/slow/synop/{yyyy}/{mm}/{dd}/{png_filename}"
            urls.append(url)
        except Exception as e:
            logger.warning("Failed to convert %s to URL: %s", png_path, e)
            continue
    return urls

##=========================
def filter_files_by_cadence(times, files, cadence_sec):
    """
    Filter (time, file) pairs to enforce a minimum time spacing (cadence).

    Parameters:
        times (list of datetime): observation times
        files (list of str): file paths (must match 1-to-1 with times)
        cadence_sec (int): minimum time spacing in seconds

    Returns:
        (filtered_times, filtered_files)
    """
    if not times or not files or len(times) != len(files):
        return times, files  # return original if mismatch

    filtered_times = [times[0]]
    filtered_files = [files[0]]
    last_time = times[0]

    for i in range(1, len(times)):
        if (times[i] - last_time).total_seconds() >= cadence_sec:
            filtered_times.append(times[i])
            filtered_files.append(files[i])
            last_time = times[i]

    return filtered_times, filtered_files

##=========================
@example.route("/api/flare/query", methods=['POST'])
def get_lwafilelist_from_database():

    start = request.form['start']
    end = request.form['end']
    cadence = request.form.get('cadence', None)
    image_type = request.form.get('image_type', 'mfs')
    print(f"image_type: {image_type}")

    cadence_sec = int(cadence) if cadence else None
    logger.info("cadence_sec: %s", cadence_sec)
    if not start or not end:
        raise ValueError("Start and end times are required.")

    file_lists, obs_times = get_lwa_file_lists_from_mysql(start, end, image_type=image_type)
    print("file_lists['slow_lev1']", len(file_lists['slow_lev1']))

    if cadence_sec:
        for key in ['slow_lev1', 'slow_lev15']:
            obs_times[key], file_lists[key] = filter_files_by_cadence(
                obs_times[key], file_lists[key], cadence_sec
            )

    logger.info("Query: Found %d spec_fits files", len(file_lists['spec_fits']))
    logger.info("Query: Found %d slow_lev1_hdf files", len(file_lists['slow_lev1']))
    logger.info("Query: Found %d slow_lev15_hdf files", len(file_lists['slow_lev15']))

    # # Convert local HDF paths to public HTTPS URLs
    # file_lists['spec_fits']   = convert_local_to_urls(file_lists['spec_fits'])
    # file_lists['slow_lev1']   = convert_local_to_urls(file_lists['slow_lev1'])
    # file_lists['slow_lev15']  = convert_local_to_urls(file_lists['slow_lev15'])

    # Convert local HDF paths to file names only
    file_lists['spec_fits']   = convert_local_to_filename(file_lists['spec_fits'])
    file_lists['slow_lev1']   = convert_local_to_filename(file_lists['slow_lev1'])
    file_lists['slow_lev15']  = convert_local_to_filename(file_lists['slow_lev15'])

    return jsonify({
        "spec_fits": file_lists['spec_fits'],
        "slow_lev1": file_lists['slow_lev1'],
        "slow_lev15": file_lists['slow_lev15']
    })

##=========================
def lwa_png_html_movie(png_paths, output_dir=f"{lwadata_dir}/{movie_subdir}"):
    ''' This routine will be called after every update to the figs_mfs
        folder (in /common/webplots/lwa-data) to write the movie.html file that 
        allows them to be viewed as a movie.  Just call this with a Time() object 
        or iso time string containing the desired date.
    '''
    # import glob, os
    # import imageio.v2 as imageio
    # try:
    #     datstr = t.iso[:10]
    # except:
    #     datstr = t[:10]
    # files = glob.glob(image_dir + '*.png')
    # files.sort()
    # nfiles = len(files)
    # if nfiles == 0:
    #     print('No files (yet) in folder',image_dir)
    #     return

    if not png_paths:
        raise ValueError("No PNG files provided.")
    files = sorted(png_paths)[:10]

    # Extract date and timestamp for HTML file naming
    fname = os.path.basename(files[0])
    if "T" not in fname:
        raise ValueError("Filename missing timestamp.")
    date_str = fname.split("T")[0].split(".")[-1]  # e.g., 2025-05-10
    timestamp_part = fname.split("T")[1].split("Z")[0]
    yyyy, mm, dd = date_str.split("-")
    html_filename = f"movie_{date_str}T{timestamp_part}Z.html"

    html_path = os.path.join(output_dir, html_filename)

    # # Copy PNG files into the output directory
    # copied_files = []
    # for f in files:
    #     try:
    #         basename = os.path.basename(f)
    #         target_path = os.path.join(output_dir, basename)
    #         shutil.copy(f, target_path)
    #         copied_files.append(target_path)
    #     except Exception as e:
    #         print(f"Failed to copy {f}: {e}")
    # files = copied_files

    ## get html from example
    files.sort()
    nfiles = len(files)
    # if nfiles == 0:
    #     print('No files (yet) in folder')#
    #     return
    ## Read one file to determine its size
    img = imread(files[0])
    ysize, xsize, ncolors = img.shape
    f = open('/nas7a/beam/software/html_movie_example.html', 'r')
    lines = f.readlines()
    nlines = len(lines)
    f.close()
    skiplines = []

    for i, line in enumerate(lines):
        k = line.find('var imax')
        j = line.find('var iwidth')
        l = line.find('NAME=animation')
        if k != -1:
            # Set number of frames
            lines[i] = line[:10] + '{:3d}'.format(nfiles) + line[13:]
        if j != -1:
            # Set width and height of images
            lines[i] = 'var iwidth = {:d}, iheight = {:d};\n'.format(xsize, ysize)
        if l != -1:
            # Set width and height of frame
            if xsize > 1125:
                # Reduce frame size for overly large images
                xfsize = xsize*3//4
                yfsize = ysize*3//4
            else:
                xfsize = xsize
                yfsize = ysize
            lines[i] = '<img NAME=animation ALT="FRAME" width='+str(xfsize)+' height='+str(yfsize)+'>'
        k = line.find('urls[')
        if k != -1:
            skiplines.append(i)
    #print skiplines
    htmlname = html_path

    f = open(htmlname, 'w')
    for i in range(skiplines[1] - 1):
        f.write(lines[i])
    for i in range(nfiles):
        rel_path = os.path.relpath(files[i], output_dir)
        f.write(f'urls[{i:d}]=url_path+"/{rel_path}";\n')
    for i in range(skiplines[-1]+1,nlines):
        f.write(lines[i])
    f.close()
    logger.info("HTML saved to %s", htmlname)
    
    movie_url = f"https://ovsa.njit.edu/lwa-data/{movie_subdir}/{html_filename}"
    return movie_url

#=========================
@example.route('/generate_html_movie', methods=['POST'])
def generate_html_movie():
    selected_files_json = request.form.get('selected_files', '')
    if not selected_files_json:
        return "No files selected", 400

    try:
        selected_files = json.loads(selected_files_json)
    except Exception as e:
        return f"Invalid JSON: {e}", 400
    logger.info("selected_files: %s", selected_files[0])

    ##===== generate a new movie.html
    png_files = convert_slow_hdf_to_existing_png(selected_files)
    # png_files = convert_png_to_urls(png_files)
    logger.info("png_files: %s", png_files[:2])
    # movie_url = generate_html_movie_from_png(png_files)
    movie_url = lwa_png_html_movie(png_files)

    logger.info("movie_url: %s", movie_url)
    logger.info("Generate_html_movie: success! %s", movie_url)

    return jsonify({"movie_url": movie_url})
    # except Exception as e:
    #     return f"Could not construct movie path: {e}", 500

##=========================
@example.route("/api/flare/spec_movie", methods=['POST'])
def get_lwa_spec_movie_from_database():
    start = request.form['start']
    if not start:
        raise ValueError("Start time is required.")
    logger.info("Start time: %s", start)

    start_time = Time(start).datetime
    date_str = start_time.strftime("%Y%m%d")
    date_str2 = start_time.strftime("%Y-%m-%d")

    ## Local server file paths (for existence check)
    # local_spec_path = f"/common/lwa/extm/daily/{date_str}.png" ?? path not exist
    local_movie_path = f"/common/webplots/lwa-data/qlook_daily/movies/slow_hdf_movie_{date_str}.mp4"

    # Public URLs
    spec_png_path = f"https://ovsa.njit.edu/lwa/extm/daily/{date_str}.png"
    movie_path = f"https://ovsa.njit.edu/lwa-data/qlook_daily/movies/slow_hdf_movie_{date_str}.mp4"

    # Check existence
    # spec_exists = os.path.exists(local_spec_path)
    movie_exists = os.path.exists(local_movie_path)

    response = {}

    response["spec_png_path"] = spec_png_path
    logger.info("spec_png_path: %s", spec_png_path)

    # if spec_exists:
    #     response["spec_png_path"] = spec_png_path
    # else:
    #     response["spec_message"] = f"The spectrogram on {date_str2} does not exist."
    if movie_exists:
        response["movie_path"] = movie_path
        logger.info("movie_path exists: %s", movie_path)
    else:
        response["movie_message"] = f"The movie on {date_str2} does not exist."
        logger.info("movie_path does not exist: %s", movie_path)

    return jsonify(response)

# ##=========================
'''Several method to downsample times
'''
def downsample(times, max_points=1000):
    if len(times) <= max_points:
        return times
    step = max(1, len(times) // max_points)
    return times[::step]

def bin_times(times, freq='1min'):
    """Convert a list of datetime objects to start times of bins."""
    if not times:
        return []
    df = pd.DataFrame({'time': pd.to_datetime(times)})
    df['binned'] = df['time'].dt.floor(freq)
    return df['binned'].drop_duplicates().tolist()

def segment_continuous_times(times, gap='1min'):
    """
    Segments input times into continuous blocks where time difference <= gap.
    Useful for Plotly line plotting with gaps.

    Parameters:
        times (list): List of datetime.datetime objects
        gap (str): A pandas-style time string (e.g., '1min', '30s')

    Returns:
        List of lists, each inner list is a continuous time segment
    """
    if not times:
        return []

    gap_td = pd.to_timedelta(gap)
    times_sorted = sorted(pd.to_datetime(times))

    segments = []
    current_segment = [times_sorted[0]]

    for t1, t2 in zip(times_sorted[:-1], times_sorted[1:]):
        if t2 - t1 <= gap_td:
            current_segment.append(t2)
        else:
            segments.append(current_segment)
            current_segment = [t2]

    segments.append(current_segment)
    return segments

def compress_time_segments(times, max_gap_seconds=60):
    """
    Reduce a list of datetime points to segments with only start and end time
    if points are within a max_gap.

    Parameters:
        times (list): List of datetime.datetime objects
        max_gap_seconds (int): Maximum gap between consecutive points to consider continuous

    Returns:
        List of (start, end) tuples representing continuous spans
    """
    if not times:
        return []

    times = sorted(times)
    max_gap = timedelta(seconds=max_gap_seconds)

    segments = []
    start = times[0]
    prev = times[0]

    for t in times[1:]:
        if t - prev > max_gap:
            segments.append((start, prev))
            start = t
        prev = t

    segments.append((start, prev))
    return segments

color_map = {
    'spec_fits': '#1f77b4',  # muted blue
    'slow_lev1':  '#ff7f0e',  # safety orange
    'slow_lev15':  '#2ca02c'   # green
}

# ##=========================
@example.route('/plot', methods=['POST'])
def plot():
    start = request.form['start']
    end = request.form['end']
    cadence = request.form.get('cadence', None)
    cadence_sec = int(cadence) if cadence else None
    logger.info("plotly cadence_sec: %s", cadence_sec)

    image_type = request.form.get('image_type', 'mfs')
    print(f"plotly image_type: {image_type}")

    try:
        start = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S") - timedelta(days=0)
        end = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S") + timedelta(days=0)
    except ValueError:
        return jsonify({'error': 'Invalid date format'}), 400
    if start > end:
        return jsonify({'error': 'End date must be after start date'}), 400

    file_lists, obs_times = get_lwa_file_lists_from_mysql(Time(start).isot, Time(end).isot, image_type=image_type)

    if cadence_sec:
        for key in ['slow_lev1', 'slow_lev15']:
            obs_times[key], file_lists[key] = filter_files_by_cadence(
                obs_times[key], file_lists[key], cadence_sec
            )

    fig = go.Figure()
    labels = ['spec_fits', 'slow_lev1', 'slow_lev15']
    # labels_fig = ['spec_fits', 'image_lev1', 'image_lev15']
    labels_fig = ['Spec', f'Image lev1_{image_type}', f'Image lev15_{image_type}']

    for i, (label, label_fig) in enumerate(zip(labels, labels_fig)):
    # for ll, label in enumerate(labels):

        times = obs_times.get(label, [])
        y_values = [label_fig] * len(times)
        label_with_count = f"N({label_fig}) = {len(times)}" if times else f"N({label_fig}) = 0"

        if label == 'spec_fits' or not times:
            # Ensure a trace is added even if there are no files
            fig.add_trace(go.Scatter(
                x=times if times else [None],
                y=y_values if times else [label_fig],
                mode='markers',
                marker=dict(size=8, color=color_map[label]),
                name=label_with_count,
                showlegend=True# if times else False
            ))
        else:
            if label == 'slow_lev1':
                segments = compress_time_segments(times, max_gap_seconds=600)#180
            else:
                segments = compress_time_segments(times, max_gap_seconds=600)

            show_legend = True
            for i_st, i_ed in segments:
                fig.add_trace(go.Scatter(
                    x=[i_st, i_ed],
                    y=[label_fig, label_fig],
                    mode='lines',
                    line=dict(width=15, color=color_map[label]),
                    name=label_with_count,
                    showlegend=show_legend
                ))
                show_legend = False

    print(f"Plot Data Availability...")

    fig.update_layout(
        title=dict(
            text='Data Availability',#'<b>Data Availability</b>',
            font=dict(size=20)#, family='Arial Black'
        ),
        xaxis_title='',#'Time'
        yaxis_title='',
        xaxis=dict(tickfont=dict(size=16), title_font=dict(size=16)),
        yaxis=dict(categoryorder='array', categoryarray=labels_fig, tickfont=dict(size=16), title_font=dict(size=16)),
        legend=dict(font=dict(size=16)),
        height=400
    )

    return jsonify({
        'plot': pio.to_json(fig)
    })

##=========================
"""To enforce user download limits (eg, max 20 downloads per day, and max 10GB per bundle)
Flask backend can track IPs.
Before serving a file, it will check how many downloads from that IP today and how much total data has been sent.
"""
def load_user_download_log():
    if os.path.exists(lwa_user_downloads_log_path):
        with open(lwa_user_downloads_log_path, 'r') as f:
            return json.load(f)
    else:
        # Create an empty log file
        with open(lwa_user_downloads_log_path, 'w') as f:
            json.dump({}, f, indent=2)
        return {}

def save_user_download_log(log):
    with open(lwa_user_downloads_log_path, 'w') as f:
        json.dump(log, f)

def is_user_download_allowed(IP, archive_size_MB, max_downloads=20, max_total_MB=50):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    log = load_user_download_log()
    IP_log = log.get(IP, {}).get(today, {"count": 0, "size": 0})
    if IP_log["count"] >= max_downloads:
        return False, f"Download count limit ({max_downloads}) reached for today. Try again tomorrow or contact the OVRO-LWA Solar Team."
    if IP_log["size"] + archive_size_MB > max_total_MB:
        # return False, f"Your requesting files are {archive_size_MB} MB, exceeded the size limit({max_total_MB} MB) for today. Try it next day or contact the OVRO-LWA solar team."
        return False, (
            f"Your requested files total approximately {int(archive_size_MB)} MB, "
            f"which exceeds the daily limit of {int(max_total_MB)} MB. "
            "Try again tomorrow or contact the OVRO-LWA Solar Team."
        )
    return True, ""

def log_user_download(IP, archive_size_MB):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    log = load_user_download_log()
    if IP not in log:
        log[IP] = {}
    if today not in log[IP]:
        log[IP][today] = {"count": 0, "size": 0}
    log[IP][today]["count"] += 1
    log[IP][today]["size"] += archive_size_MB
    save_user_download_log(log)

# ##=========================
def extract_timestamp_from_filename(filename):
    """
    Extract YYYY-MM-DDTHHMMSSZ-like timestamp from filename and convert to YYYYMMDDTHHMMSS.
    If not found, check for a simpler YYYYMMDD pattern and return the filename if matched.
    Otherwise, return 'UNKNOWN'.
    """
    match = re.search(r"\d{4}-\d{2}-\d{2}T\d{6}", filename)
    match_spec = re.search(r"\d{8}", filename)  # Matches YYYYMMDD

    if match:
        return match.group(0).replace("-", "").replace(":", "")
    elif match_spec:
        return filename.split(".fits")[0]
    else:
        return "UNKNOWN"

# ##=========================
@example.route('/generate_bundle/<bundle_type>', methods=['POST'])
def generate_data_bundle(bundle_type):
    start = request.form.get('start')
    end = request.form.get('end')

    cadence = request.form.get('cadence', None)
    cadence_sec = int(cadence) if cadence else None

    image_type = request.form.get('image_type', 'mfs')

    if not start or not end:
        return "Start and end parameters are required", 400

    file_lists, obs_times = get_lwa_file_lists_from_mysql(start, end, image_type=image_type)

    if bundle_type not in file_lists:
        return f"Invalid bundle type: {bundle_type}", 400

    if cadence_sec and bundle_type in ['slow_lev1', 'slow_lev15']:
        obs_times[bundle_type], file_lists[bundle_type] = filter_files_by_cadence(
            obs_times[bundle_type], file_lists[bundle_type], cadence_sec
        )

    # for selected_files
    selected_files_json = request.form.get('selected_files')
    if selected_files_json:
        try:
            selected_files = set(json.loads(selected_files_json))
            file_paths = [f for f in file_lists[bundle_type] if os.path.basename(f) in selected_files]
        except Exception as e:
            return f"Invalid selected_files format: {e}", 400
    else:
        file_paths = file_lists[bundle_type]

    # file_paths = file_lists[bundle_type]
    if not file_paths:
        return f"No files found for {bundle_type}", 404

    ### Limiting download to first 10 files
    # if len(file_paths) > 10:
    #     file_paths = file_paths[:10]
    #     logger.info("Limiting download to first 10 files out of %d", len(file_paths))
    # if bundle_type == 'spec_fits':
    #     file_paths = file_paths[:1]  # Keep only the first file

    # To check if the data request for downloading is allowed
    user_IP = request.remote_addr
    estimated_size_MB = sum(os.path.getsize(f) for f in file_paths if os.path.exists(f)) / (1024 * 1024)
    allowed, reason = is_user_download_allowed(user_IP, estimated_size_MB, max_downloads=max_IP_downloads_per_day, max_total_MB=max_MB_downloads_per_IP)
    if not allowed:
        return f"Download denied: {reason}", 403
    log_user_download(user_IP, estimated_size_MB)

    # Download is allowed
    bundle_names = {
        'spec_fits': 'ovro-lwa-spec',
        'slow_lev1': 'ovro-lwa-image-lev1',
        'slow_lev15': 'ovro-lwa-image-lev15'
    }
    start_time_str = extract_timestamp_from_filename(os.path.basename(file_paths[0]))
    end_time_str = extract_timestamp_from_filename(os.path.basename(file_paths[-1]))
    cadence_suffix = f"_cad{cadence_sec}s" if cadence_sec else ""

    archive_label = bundle_names.get(bundle_type, bundle_type)
    archive_filename = f"{archive_label}_{start_time_str}_{end_time_str}{cadence_suffix}.tar.gz"
    # # Create a permanent bundle output path
    # bundle_dir = "/data1/xychen/flaskenv/lwa_data_query_request"
    bundle_dir = f"{lwadata_dir}/{data_subdir}"
    os.makedirs(bundle_dir, exist_ok=True)
    # archivse_path = os.path.join(bundle_dir, f"{bundle_type}_{start_time_str}_{end_time_str}.tar.gz")
    archive_path = os.path.join(bundle_dir, archive_filename)

    # Create .tar
    if not os.path.exists(archive_path):
        with tempfile.TemporaryDirectory() as temp_dir:
            for path in file_paths:
                if os.path.exists(path):
                    shutil.copy(path, temp_dir)
            shutil.make_archive(
                base_name=archive_path.replace(".tar.gz", ""),
                format="gztar",
                root_dir=temp_dir
            )
    logger.info("Generate bundle for %s from %s to %s", bundle_type, start, end)
    return jsonify({"archive_name": os.path.basename(archive_path)})

# ##=========================
@example.route('/download_ready_bundle/<archive_name>', methods=['GET'])
def download_ready_bundle(archive_name):
    # bundle_dir = "/data1/xychen/flaskenv/lwa_dafta_query_request"
    bundle_dir = f"{lwadata_dir}/{data_subdir}"
    archive_path = os.path.join(bundle_dir, archive_name)
    logger.info("Download bundle: %s", archive_path)
    if os.path.exists(archive_path):
        return send_file(archive_path, as_attachment=True, download_name=archive_name)
    else:
        return f"{archive_name} not found", 404

# ##=========================
@example.route("/")
def render_example_paper():
    hostname = socket.gethostname()
    return render_template('index.html', result=[], plot_html_ID=None, hostname=hostname)
