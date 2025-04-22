#!/bin/bash                                                             


set -e

MERGED_FILES="all_summaries.csv"
SUMMARY_FILES=$(ls summary_*.csv 2> /dev/null)

# Check if any files are found                                          
if [ -z "$SUMMARY_FILES" ]; then
  echo "ummary_*.csv files  not found."
fi


FIRST_FILE=$(echo $SUMMARY_FILES | awk '{print $1}')
head -n 1 "$FIRST_FILE" > "$MERGED_FILE"


for file in $SUMMARY_FILES; do
  echo "Adding $file..."
  tail -n +2 "$file" >> "$MERGED_FILE"
done

echo "Merge complete. Output: $MERGED_FILE"
