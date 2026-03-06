#!/bin/sh
set -e

# Rscript scripts/check_new_release.R

# mkdir -p inputs
# curl "https://fileshare.eoir.justice.gov/EOIR%20Case%20Data.zip" -o eoir_data.zip

# unzip -o -j eoir_data.zip -d inputs
# # rm eoir_data.zip
# mkdir -p outputs

Rscript scripts/eoir_appeals.R
Rscript scripts/eoir_associated_bond.R
Rscript scripts/eoir_case.R
Rscript scripts/eoir_court_applications.R
Rscript scripts/eoir_custody_history.R
Rscript scripts/eoir_lookups.R
Rscript scripts/eoir_proceeding.R
Rscript scripts/eoir_proceedings_charges.R
Rscript scripts/eoir_case_joins.R