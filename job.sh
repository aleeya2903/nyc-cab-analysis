#!/bin/bash
# process_local_parquet.sh

set -e

PARQUET_FILE="$1"
BASENAME=$(basename "$PARQUET_FILE" .parquet)
OUTFILE="summary_${BASENAME}.csv"

echo "Processing $PARQUET_FILE..."

python3 - <<EOF
import pandas as pd

df = pd.read_parquet("$PARQUET_FILE")
df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
df["trip_duration_min"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60
df["rush_hour"] = df["tpep_pickup_datetime"].dt.hour.isin([7,8,9,16,17,18,19])
df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
df["pickup_dayofweek"] = df["tpep_pickup_datetime"].dt.dayofweek
df["is_holiday"] = df["tpep_pickup_datetime"].dt.strftime('%Y-%m-%d').isin(["2009-01-01"])

summary = pd.DataFrame([{
    "file": "$PARQUET_FILE",
    "n_rows": len(df),
    "rush_hour_avg_fare": df[df["rush_hour"]]["Fare_amount"].mean(),
    "non_rush_avg_fare": df[~df["rush_hour"]]["Fare_amount"].mean(),
    "holiday_trip_count": int(df["is_holiday"].sum()),
    "avg_trip_distance": df["Trip_distance"].mean(),
    "fare_vs_distance_corr": df["Fare_amount"].corr(df["Trip_distance"]),
    "airport_trip_count": int(df["RateCodeID"].isin([2,6]).sum()),
    "duration_vs_distance_corr": df["trip_duration_min"].corr(df["Trip_distance"]),
    "congestion_surcharge_total": df["Congestion_Surcharge"].sum(),
    "group_ride_count": int((df["RateCodeID"] == 6).sum()),
    "total_revenue": df["Total_amount"].sum(),
    "anomalous_fares_count": int((df["Fare_amount"] > 200).sum()),
    "rush_hour_extra_avg": df[df["rush_hour"]]["Extra"].mean()
}])

summary.to_csv("$OUTFILE", index=False)
EOF

echo "Summary written to $OUTFILE"
