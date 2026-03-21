#!/bin/sh
set -e

# mkdir -p inputs_eoir
# curl "https://fileshare.eoir.justice.gov/EOIR%20Case%20Data.zip" -o eoir_data.zip

# unzip -o -j eoir_data.zip -d inputs_eoir
# rm eoir_data.zip
# mkdir -p tmp
# mkdir -p outputs

mkdir -p logs
LOGFILE="logs/download_clean_$(date +%Y%m%d_%H%M%S).log"

{
Rscript scripts/geography_join.R
Rscript scripts/eoir_appeals.R
Rscript scripts/eoir_associated_bond.R
Rscript scripts/eoir_case.R
Rscript scripts/eoir_court_applications.R
Rscript scripts/eoir_custody_history.R
Rscript scripts/eoir_lookups.R
Rscript scripts/eoir_proceeding.R
Rscript scripts/eoir_proceedings_charges.R
Rscript scripts/eoir_case_joins.R
} 2>&1 | tee "$LOGFILE"