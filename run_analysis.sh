#!/bin/bash
# run_analysis.sh: wrapper to launch Analyze.R

INFILE="$1"
OUTFILE="$2"
ZONES="$3"
mkdir -p $(dirname "$OUTFILE")
# Run directly since HTCondor manages the container
Rscript ./Analyze.R "$INFILE" "$OUTFILE" "$ZONES"
if [ $? -ne 0 ]; then
    echo "Error: R script failed"
    exit 1
fi

