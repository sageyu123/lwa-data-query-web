# LWA Data Query Web Interface

This repository contains a Flask-based web application for querying and visualizing data products from the OVRO-LWA (Owens Valley Radio Observatory - Long Wavelength Array). The system supports efficient database lookups for spectrogram and HDF imaging data and enables quick preview of available observations.

## Features

- Query LWA observation metadata stored in MySQL.
- Display available `.fits`, `lev1` and `lev15` HDF files.
- Preview quicklook spectrograms and generate daily imaging movies.
- Auto movie generation for selected dates using `ffmpeg`.

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/xingyaochen0/lwa-data-query-web.git
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
FLARE_DB_DATABASE=lwa_metadata_query1
```

Or export them manually in your terminal before running.

### 4. Run the App Locally

```bash
python wsgi.py
```

This will run the app on `http://127.0.0.1:2001`.

## Deployment

For production deployment (e.g., `https://ovsa.njit.edu/lwadata-query/`), you may use `gunicorn`, Apache with mod_wsgi, or Nginx with a reverse proxy. Make sure the `static` and movie generation paths are correctly permissioned and mounted.

## Folder Structure

- `wsgi.py` — entry point for running the app
- `routes.py` — defines app structure and blueprint
- `blueprints/example` — main application logic and routes
- `static/movies/` — stores generated MP4 movies
- `templates/` — HTML templates
- `utils/` — utility scripts for querying and movie generation

## License

MIT License. See [LICENSE](LICENSE) for details.

## Maintainer

Xingyao Chen – [xingyaochen0@github](https://github.com/xingyaochen0)
