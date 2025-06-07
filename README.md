# LWA Data Query Web Interface

This repository contains a Flask-based web application for querying and visualizing solar data products from the [**OVRO-LWA Solar Data Pipeline**](https://github.com/ovro-eovsa/ovro-lwa-solar) (Owens Valley Radio Observatory - Long Wavelength Array). The system supports efficient database lookups for spectrogram and HDF imaging data and enables quick preview of available observations.

<img width="622" alt="image" src="https://github.com/user-attachments/assets/82743200-191d-450c-a8b7-0dd4586d64c4" />


## Features

- Query OVRO-LWA observation metadata information stored in MySQL.
- Display available `Beamforming Spectrograms (FITS)`, `Level 1 Spectral Images (HDF5)` and `Level 1.5 Spectral Images (HDF5)` files.
- Preview daily spectrogram plot and multi-frequency imaging movies.


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

## Detailed Documentation

### 1. Query Interface for Solar Data Files

Users can specify a custom **`time range`** (e.g., in ISO format), **`time cadence`** in seconds (default: 10s), **`image type`** (either **Band Averaged (mfs)** or **Fine Channel (fch)**) and retrieve the available file names for three key OVRO-LWA data products:

- **`Beamforming Spectrograms (FITS)`**:  
  Daily spectrogram FITS files from beam-formed data, with a **time resolution** of ~??s and a **frequency resolution** of ~?? kHz

- **`Level 1 Spectral Images (HDF5)`**:  
  Level-1 solar imaging mfs/fch HDF5 files, providing multi-frequency images integrated over 10 seconds.  
  - These can be converted to FITS format using the utility of [`recover_fits_from_h5`](https://github.com/ovro-eovsa/ovro-lwa-solar/blob/a9521ca5d4695c7fabf03e88aced5cf636d72ebe/ovrolwasolar/utils.py#L781)

- **`Level 1.5 Spectral Images (HDF5)`**:  
  Level-1.5 imaging mfs/fch files after applying ionospheric **refraction correction**.  
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

### 3. Daily Quicklook: Spectrogram Plot and Multi-Frequency Movie

At the bottom of the page, a "Quicklook" section displays:

- **Spectrogram Plot**: A visual overview of daily beam-formed intensity data.
- **Multi-Frequency Movie**: A snapshot-based animation from imaging PNG files.

If a daily movie (named as `slow_hdf_movie_YYYYMMDD.mp4`) is not found, the website will display the message: “The movie on YYYY-MM-DD does not exist.”.
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


