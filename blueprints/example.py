import numpy as np
import pandas as pd
from flask import Flask, Blueprint, render_template, request, jsonify, url_for
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

import socket
import json
import plotly
import os
import mysql.connector
from astropy.time import Time
import requests

from datetime import datetime, timedelta
from glob import glob
import subprocess
import tempfile
import shutil
from pathlib import Path

##=========================
example = Blueprint('example', __name__, template_folder='templates')

##=========================
def create_lwa_query_db_connection():
    return mysql.connector.connect(
        host=os.getenv('FLARE_DB_HOST'),
        database='lwa_metadata_query',
        user=os.getenv('FLARE_DB_USER'),
        password=os.getenv('FLARE_DB_PASSWORD')
    )

##=========================
def get_lwa_file_lists_from_mysql(start_utc, end_utc):
    start = Time(start_utc).datetime
    end = Time(end_utc).datetime
    connection = create_lwa_query_db_connection()
    cursor = connection.cursor()
    query = """
        SELECT file_path, obs_time FROM {table}
        WHERE obs_time BETWEEN %s AND %s
        ORDER BY obs_time
    """
    # tables = {
    #     'fast_hdf': 'lwa_fast_hdf_files',
    #     'slow_hdf': 'lwa_slow_hdf_files',
    #     'spec_fits': 'lwa_spec_fits_files'
    # }
    tables = {
        'spec_fits': 'lwa_spec_fits_files',
        'slow_lev1': 'lwa_slow_lev1_hdf_files',
        'slow_lev15': 'lwa_slow_lev15_hdf_files'
    }
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
# output_name="slow_hdf_movie.mp4"
def generate_movie_from_pngs(png_files, output_name=None):
    if not png_files:
        return None

    if not output_name:
        date_str = os.path.basename(png_files[0]).split("T")[0].split('.')[-1].replace("-", "")
        output_name = f"slow_hdf_movie_{date_str}_sub.mp4"

    temp_dir = tempfile.mkdtemp()
    try:
        # Symlink all PNGs into temp dir with simple names: 0001.png, 0002.png...
        for i, f in enumerate(sorted(png_files)):
            link_name = os.path.join(temp_dir, f"{i:04d}.png")
            os.symlink(f, link_name)

        output_path = os.path.join("static", "movies", output_name)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        cmd = [
            "ffmpeg", "-y",
            "-framerate", "6",
            "-i", os.path.join(temp_dir, "%04d.png"),
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            output_path
        ]
        subprocess.run(cmd, check=True)
        return "/" + output_path
    except Exception as e:
        print("Movie generation failed:", e)
        return None
    finally:
        shutil.rmtree(temp_dir)

##=========================
def convert_slow_hdf_to_existing_png(hdf_list):
    """
    Convert each .hdf path to its corresponding .png path by naming pattern,
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
            # Extract date from the filename
            if "T" in hdf_filename:
                # timestamp_part = hdf_filename.split("T")[1]
                date_part = hdf_filename.split("T")[0].split('.')[-1]  # e.g., '2025-04-10'
                yyyy, mm, dd = date_part.split('-')
            else:
                continue  # skip if filename format unexpected

            png_filename = hdf_filename.replace('.lev1.5_', '.synop_').replace('.hdf', '.png')
            # Full target .png path
            png_path = f"/common/webplots/lwa-data/qlook_images/slow/synop/{yyyy}/{mm}/{dd}/{png_filename}"
            print(f"png_path: {png_path}")

            if os.path.exists(png_path):
                png_list.append(png_path)
        except Exception as e:
            print(f"Error processing {hdf_path}: {e}")
            continue
    return png_list
# png_files = convert_slow_hdf_to_existing_png(slow_hdf_files)





##=========================
@example.route("/api/flare/query", methods=['POST'])
def get_lwafilelist_from_database():

    start = request.form['start']
    end = request.form['end']
    # print("start_date22", start, end)
    if not start or not end:
        raise ValueError("Start and end times are required.")

    file_lists, _ = get_lwa_file_lists_from_mysql(start, end)

    print(f"Found {len(file_lists['spec_fits'])} spec_fits files")
    print(f"Found {len(file_lists['slow_lev1'])} slow_lev1_hdf files")
    print(f"Found {len(file_lists['slow_lev15'])} slow_lev15_hdf files")

    # Convert local HDF paths to public HTTPS URLs
    file_lists['spec_fits']   = convert_local_to_urls(file_lists['spec_fits'])
    file_lists['slow_lev1']   = convert_local_to_urls(file_lists['slow_lev1'])
    file_lists['slow_lev15']  = convert_local_to_urls(file_lists['slow_lev15'])

    return jsonify({
        "spec_fits": file_lists['spec_fits'],
        "slow_lev1": file_lists['slow_lev1'],
        "slow_lev15": file_lists['slow_lev15']
    })

    # slow_hdf_files = (file_lists['slow_hdf'])[:20]
    # img_png_files = convert_slow_hdf_to_existing_png(slow_hdf_files)
    # movie_path = generate_movie_from_pngs(img_png_files)
    # print(f"movie_path: {movie_path}")
    # # print(f"png_files: {img_png_files}")
    # spec_png_files = "https://ovsa.njit.edu/lwa/extm/daily/" + os.path.basename(img_png_files[0]).split("T")[0].split('.')[-1].replace('-', '') + '.png'
    # print(f"spec_png_files: {spec_png_files}")

    # return jsonify({
    # "spec_fits": file_lists['spec_fits'],
    # "slow_hdf": file_lists['slow_hdf'],
    # "fast_hdf": file_lists['fast_hdf'],
    # "movie_path": movie_path,
    # "spec_png_path":spec_png_files
    # })



##=========================
def get_spec_and_movie_paths(slow_hdf_files):
    png_files = convert_slow_hdf_to_existing_png(slow_hdf_files)
    movie_path = generate_movie_from_pngs(png_files) if png_files else None
    # if movie_path:
    #     cache_buster = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    #     movie_path += f"?v={cache_buster}"
    try:
        date_str = os.path.basename(png_files[0]).split("T")[0].split('.')[-1].replace("-", "")
        spec_png = f"https://ovsa.njit.edu/lwa/extm/daily/{date_str}.png"
    except Exception as e:
        spec_png = None
        print("Spec image link generation failed:", e)

    return spec_png, movie_path

##=========================
@example.route("/api/flare/spec_movie", methods=['POST'])
def get_lwa_spec_movie_from_database():
    start = request.form['start']
    if not start:
        raise ValueError("Start time is required.")
    print("start111", start)
    # ## Method 1: Generate image movies for the given time range
    # start_time = Time(start).datetime
    # end_time = start_time + timedelta(hours=2)
    # file_lists, _ = get_lwa_file_lists_from_mysql(start_time.isoformat(), end_time.isoformat())
    # slow_hdf_files = file_lists['slow_lev15']#)[:20]
    # spec_png_path, movie_path = get_spec_and_movie_paths(slow_hdf_files)

    start_time = Time(start).datetime
    date_str = start_time.strftime("%Y%m%d")
    movie_filename = f"slow_hdf_movie_{date_str}.mp4"
    movie_path_local = os.path.join("static", "movies", movie_filename)

    if os.path.exists(movie_path_local):
        # Method 2: Return the existing movie if available
        movie_path = f"/static/movies/{movie_filename}"
        spec_png_path = f"https://ovsa.njit.edu/lwa/extm/daily/{date_str}.png"
    else:
        # Method 1: Generate movie dynamically from PNGs
        end_time = start_time + timedelta(hours=6)
        file_lists, _ = get_lwa_file_lists_from_mysql(start_time.isoformat(), end_time.isoformat())
        slow_hdf_files = file_lists['slow_lev15']#)[:20]
        spec_png_path, movie_path = get_spec_and_movie_paths(slow_hdf_files)
    ##
    print("spec_png_path", spec_png_path)
    print("movie_path", movie_path)
    return jsonify({
        "movie_path": movie_path,
        "spec_png_path": spec_png_path
    })






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

    try:
        start = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S") - timedelta(days=0)
        end = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S") + timedelta(days=0)
    except ValueError:
        return jsonify({'error': 'Invalid date format'}), 400

    if start > end:
        return jsonify({'error': 'End date must be after start date'}), 400

    file_lists, obs_times = get_lwa_file_lists_from_mysql(Time(start).isot, Time(end).isot)

    fig = go.Figure()
    labels = ['spec_fits', 'slow_lev1', 'slow_lev15']
    labels_fig = ['spec_fits', 'image_lev1', 'image_lev15']

    # for label in labels:
    #     times = obs_times.get(label, [])
    #     y_values = [label] * len(times)

    #     label_with_count = f"N({label}) = {len(times)}" if times else f"N({label}) = 0"

    #     # Ensure a trace is added even if there are no files
    #     fig.add_trace(go.Scatter(
    #         x=times if times else [None],
    #         y=y_values if times else [label],
    #         mode='markers',
    #         marker=dict(size=8),
    #         name=label_with_count,
    #         showlegend=True# if times else False
    #     ))


    # for label in labels:
    #     times = obs_times.get(label, [])
    #     times_ds = downsample(times)

    #     label_with_count = f"N({label}) = {len(times)}"

    #     fig.add_trace(go.Scatter(
    #         x=times_ds if times_ds else [None],
    #         y=[label] * len(times_ds) if times_ds else [label],
    #         mode='markers',
    #         marker=dict(size=6),
    #         name=label_with_count,
    #         showlegend=True
    #     ))



    # for label in labels:
    #     times = obs_times.get(label, [])
    #     binned_times = bin_times(times, freq='1min')
    #     y_values = [label] * len(binned_times)

    #     label_with_count = f"N({label}) = {len(times)}"

    #     fig.add_trace(go.Scatter(
    #         x=binned_times if binned_times else [None],
    #         y=y_values if binned_times else [label],
    #         mode='markers',
    #         marker=dict(size=6),
    #         name=label_with_count
    #     ))


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
            # segments = segment_continuous_times(times, gap='1min')
            # # for seg in segments:
            # #     fig.add_trace(go.Scatter(
            # #         x=seg if seg else [None],
            # #         y=[label] * len(seg),
            # #         mode='lines',
            # #         line=dict(width=4, color=color_map[label]),
            # #         name=label_with_count,
            # #         showlegend=(seg == segments[0])  # Show legend only once per label
            # #     ))

            # for i, seg in enumerate(segments):
            #     fig.add_trace(go.Scatter(
            #         x=seg,
            #         y=[label] * len(seg),
            #         mode='lines+markers',
            #         line=dict(width=5, color=color_map[label]),
            #         # marker=dict(size=3, color=color_map[label]),
            #         name=label_with_count,
            #         showlegend=(i == 0)
            #     ))
            if label == 'slow_lev1':
                segments = compress_time_segments(times, max_gap_seconds=600)#180
            else:
                segments = compress_time_segments(times, max_gap_seconds=300)

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


            # segments = compress_time_segments(times, max_gap_seconds=60)
            # show_legend = True
            # min_duration = timedelta(seconds=30)

            # for i_st, i_ed in segments:
            #     # Enforce minimum visual duration
            #     if (i_ed - i_st) < min_duration:
            #         i_ed = i_st + min_duration

            #     fig.add_trace(go.Scatter(
            #         x=[i_st, i_ed],
            #         y=[label, label],
            #         mode='lines',
            #         line=dict(width=15, color=color_map[label], shape='hv'),
            #         name=label_with_count,
            #         showlegend=show_legend
            #     ))
            #     show_legend = False

    print(f"Plot Data Availability...")

    fig.update_layout(
        title='Data Availability',
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







# ##=========================
@example.route("/")
def render_example_paper():
    hostname = socket.gethostname()
    return render_template('index.html', result=[], plot_html_ID=None, hostname=hostname)