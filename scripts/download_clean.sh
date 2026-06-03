#!/bin/sh
set -e

# mkdir -p inputs_eoir
# curl "https://fileshare.eoir.justice.gov/EOIR%20Case%20Data.zip" -o eoir_data.zip

# unzip -o -j eoir_data.zip -d inputs_eoir
# rm eoir_data.zip
# mkdir -p tmp
# mkdir -p outputs

mkdir -p logs tmp
STAMP="$(date +%Y%m%d_%H%M%S)"
LOGFILE="logs/download_clean_${STAMP}.log"

# Stage 1: producers — each writes a unique tmp/*.parquet, independent of the others.
# Fan out in parallel; per-script logs avoid interleaved output.
PRODUCERS="
  scripts/geography_join.R
  scripts/eoir_appeals.R
  scripts/eoir_associated_bond.R
  scripts/eoir_case.R
  scripts/eoir_court_applications.R
  scripts/eoir_custody_history.R
  scripts/eoir_lookups.R
  scripts/eoir_proceeding.R
  scripts/eoir_proceedings_charges.R
"

pids=""
for s in $PRODUCERS; do
  name=$(basename "$s" .R)
  echo "[$(date +%H:%M:%S)] starting $name" | tee -a "$LOGFILE"
  Rscript "$s" > "logs/${name}_${STAMP}.log" 2>&1 &
  pids="$pids $!:$name"
done

failed=0
for entry in $pids; do
  pid="${entry%%:*}"
  name="${entry##*:}"
  if wait "$pid"; then
    echo "[$(date +%H:%M:%S)] done    $name" | tee -a "$LOGFILE"
  else
    echo "[$(date +%H:%M:%S)] FAILED  $name (see logs/${name}_${STAMP}.log)" | tee -a "$LOGFILE" >&2
    failed=1
  fi
done

# Stitch per-script logs into the combined logfile.
for s in $PRODUCERS; do
  name=$(basename "$s" .R)
  {
    echo
    echo "===== $name ====="
    cat "logs/${name}_${STAMP}.log"
  } >> "$LOGFILE"
done

if [ "$failed" -ne 0 ]; then
  echo "Producer stage had failures; aborting joins." | tee -a "$LOGFILE" >&2
  exit 1
fi

# Stage 2: join — depends on every tmp/*.parquet above.
Rscript scripts/eoir_case_joins.R 2>&1 | tee -a "$LOGFILE"