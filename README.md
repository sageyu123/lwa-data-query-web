# LWA Data Query Web Interface

This repository contains a Flask-based web application for querying and visualizing solar data products from the [**OVRO-LWA Solar Data Pipeline**](https://github.com/ovro-eovsa/ovro-lwa-solar) (Owens Valley Radio Observatory - Long Wavelength Array). The system supports efficient database lookups for spectrogram and HDF imaging data and enables quick preview of available observations.

<img width="624" alt="image" src="https://github.com/user-attachments/assets/c92445e9-0f00-4858-a0c8-cca11c2689c2" />

## Features

- Query LWA observation metadata information stored in MySQL.
- Display available `spec_fits`, `image_lev1` and `image_lev15` HDF files.
- Preview quicklook spectrograms and daily imaging movies.

<!---
- Auto movie generation for selected dates using `ffmpeg`.
-->

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/ovro-eovsa/lwa-data-query-web.git
cd lwa-data-query-web
```

### 2. Set Up Python Environment

Use a virtual environment if desired, then install dependencies:

```bash
pip install -r requirements.txt
```

### 3. Set Environment Variables

Create a `.env` file with the following:

```env
FLARE_DB_HOST=your_mysql_host
FLARE_DB_USER=your_db_user
FLARE_DB_PASSWORD=your_db_password
FLARE_DB_DATABASE=lwa_metadata_query
```

Or export them manually in your terminal before running.

### 4. Run the App Locally

```bash
python wsgi.py
```

This will run the app on `http://127.0.0.1:2001`.

## Deployment

For production deployment (e.g., `https://ovsa.njit.edu/lwadata-query/`), you may use `gunicorn`, Apache with mod_wsgi, or Nginx with a reverse proxy. Make sure the `static` and movie generation paths are correctly permissioned and mounted.

## Project Structure

- `wsgi.py` — entry point for running the app
- `routes.py` — defines app structure and blueprint
- `blueprints/example` — main application logic and routes
- `static/movies/` — stores generated MP4 movies
- `templates/` — HTML templates
- `utils/` — utility scripts for metadata maintenance and movie generation

## License

MIT License. See [LICENSE](LICENSE) for details.

## Acknowledgments

This tool leverages the **OVRO-LWA Solar Data Processing Pipeline**, developed in collaboration with [OVSA-NJIT](https://www.ovsa.njit.edu/) and [OVRO](https://www.ovro.caltech.edu/). Special thanks to the developers of [`ovro-lwa-solar`](https://github.com/ovro-eovsa/ovro-lwa-solar).

<!---
## Maintainer

Xingyao Chen – [xingyaochen0@github](https://github.com/xingyaochen0)
-->


---

## ✨ Detailed Documentation

### 1. Query Interface for Solar Data Files

Users can specify a custom time range, time cadence, and retrieve the available file names for three key OVRO-LWA data products:

- **`spec_fits`**:  
  Daily spectrogram FITS files from beam-formed data, with a **time resolution** of ~??s and a **frequency resolution** of ~?? kHz

- **`image_lev1`**:  
  Level-1 solar imaging HDF5 files, providing multi-frequency images integrated over 10 seconds.  
  - These can be converted to FITS format using the utility of [`recover_fits_from_h5`](https://github.com/ovro-eovsa/ovro-lwa-solar/blob/a9521ca5d4695c7fabf03e88aced5cf636d72ebe/ovrolwasolar/utils.py#L781)

- **`image_lev15`**:  
  Level-1.5 imaging files after applying ionospheric **refraction correction**.  
  - Refraction-corrected output using: [`refraction_correction`](https://github.com/ovro-eovsa/ovro-lwa-solar/blob/main/ovrolwasolar/refraction_correction.py)

It allows users to **select data files**, **Generate .tar**, **Download .tar**, and **Generate movie** with HTML format from those image data files.
<!---
Each category allows users to download a corresponding `.txt` list of URLs for automated downloading.

#### Example WGET Commands

```bash
# Download all spec_fits files
wget -i ovro-lwa_solar_spec_fits.txt
```
```bash
# Save to a specific directory
wget -P /your/download/path -i ovro-lwa_solar_spec_fits.txt
```
```bash
# Resume interrupted downloads
wget -c -i ovro-lwa_solar_spec_fits.txt
```
-->


### 2. Interactive Data Availability Overview

The middle section of the page visualizes the **temporal coverage** of each file type within the selected time range.

- Zoom-in capabilities allow precise inspection of data availability.
- The legend displays the total number of files per category.
- To handle high file counts and enhance responsiveness, the plotting is compressed using a custom `compress_time_segments()` function (see: [example.py](https://github.com/xingyaochen0/lwa-data-query-web/blob/main/blueprints/example.py)).

### 3. Daily Quicklook: Spectrogram and Movie

At the bottom of the page, a "Quicklook" section displays:

- **Spectrogram**: A visual overview of daily beam-formed intensity data.
- **Imaging Movie**: A snapshot-based animation from imaging PNG files.

If a daily movie (named as `slow_hdf_movie_YYYYMMDD.mp4`) is not found, the server will automatically generate one in the background for the time interval **12:00–17:00 UT** of the selected start date, named as `slow_hdf_movie_YYYYMMDD_sub.mp4`, and store it in `/static/movies/`.
Users can interactively use the **−1 Day** / **+1 Day** buttons to view the spectrogram and movie from adjacent days, **slide the movie playback bar**, and **download the resulting movie**.




---

## Metadata Maintenance

The metadata is maintained in a MySQL DATABASE:

```
USE lwa_metadata_query;
```

To populate or update the metadata:

```bash
# Add entries for a given range (e.g., in a daily cron job)
python lwadata2sql.py --start 2025-04-01T00:00:00 --end 2025-05-01T00:00:00
```
```bash
# Manually delete records for a time range
python lwadata2sql.py --start 2025-04-30T00:00:00 --end 2025-05-01T00:00:00 --delete
```

---

## Daily Movie Cronjob

A cron job can also be configured to automatically generate daily movies stored in `/static/movies/`, using imaging PNGs from [https://ovsa.njit.edu/lwa-data/qlook_images/slow/synop/](https://ovsa.njit.edu/lwa-data/qlook_images/slow/synop/).

You can also use the script directly via command line:

```bash
# Generate movie for given date range
python lwa-query-web_utils.py --gen movie --start 2025-04-25 --end 2025-05-01
```

The output movies are named `slow_hdf_movie_YYYYMMDD.mp4`. Each full-day movie may last **~5 minutes** and take approximately **20 MB** of disk space.


## Automatic Cleanup

A cron job is set up on _ovsa_ to run `cleanup_tmp.sh` every hour. It recursively removes:

- `.tar.gz` files under `/common/webplots/lwa-data/tmp/data-request/`
- `.html` files under `/common/webplots/lwa-data/tmp/html/`

that are older than 24 hours, helping free up space in the temporary storage area.


