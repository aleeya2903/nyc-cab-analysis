#!/bin/bash
set -e

PARQUET_FILE="$1"
echo "Processing $PARQUET_FILE..."

Rscript Analyze.R "$PARQUET_FILE"
