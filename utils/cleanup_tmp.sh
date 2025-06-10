#!/bin/bash

# Set base directory
BASE_DIR="/common/webplots/lwa-data/tmp"

# Default age limit in hours
AGE_LIMIT_HOURS=24

usage() {
    echo "Usage: $0 [AGE_LIMIT_HOURS]"
    echo ""
    echo "Deletes .tar.gz files under data-request/ and .html files under html/"
    echo "that are older than AGE_LIMIT_HOURS. Default is 24 hours if not specified."
    echo ""
    echo "Options:"
    echo "  --help         Show this help message and exit"
    echo ""
    echo "Example:"
    echo "  $0             # Deletes files older than 24 hours"
    echo "  $0 12          # Deletes files older than 12 hours"
}

# Parse arguments
if [[ "$1" == "--help" ]]; then
    usage
    exit 0
elif [[ -n "$1" ]]; then
    if ! [[ "$1" =~ ^[0-9]+$ ]]; then
        echo "Error: AGE_LIMIT_HOURS must be an integer."
        usage
        exit 1
    fi
    AGE_LIMIT_HOURS="$1"
fi

# Convert hours to minutes
AGE_LIMIT_MINUTES=$((AGE_LIMIT_HOURS * 60))

# Delete old .tar.gz files
find "$BASE_DIR"/data-request -type f -name "*.tar.gz" -mmin +$AGE_LIMIT_MINUTES -exec rm -f {} \;

# Delete old .html files
find "$BASE_DIR"/html -type f -name "*.html" -mmin +$AGE_LIMIT_MINUTES -exec rm -f {} \;
