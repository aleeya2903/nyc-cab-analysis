#!/usr/bin/env Rscript
# analyze_taxi_data.R
#
# Usage:
#   Rscript analyze_taxi_data.R \
#     /staging/groups/STAT_DSCP/group13_2025/yellow_tripdata_2025-01.parquet \
#     /home/groups/STAT_DSCP/group13_2025/results/summary_2025-01.rds \
#     /home/groups/STAT_DSCP/group13_2025/taxi_zones.csv

args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 3) {
  stop("Usage: analyze_taxi_data.R <in.parquet> <out.rds> <zones.csv>")
}

infile  <- args[1]
outfile <- args[2]
zones_csv <- args[3]

library(arrow)
library(dplyr)
library(lubridate)
library(broom)

# 1. load zone lookup
zones <- read.csv(zones_csv, stringsAsFactors = FALSE) %>%
  rename(PULocationID = LocationID, Borough = borough)

# 2. read only needed columns
df <- read_parquet(infile, col_select = c(
  "tpep_pickup_datetime","tpep_dropoff_datetime",
  "trip_distance","fare_amount","total_amount",
  "PULocationID","DOLocationID"
))

# 3. compute duration & rush flag, filter
df2 <- df %>%
  mutate(
    pickup   = ymd_hms(tpep_pickup_datetime),
    dropoff  = ymd_hms(tpep_dropoff_datetime),
    trip_duration = as.numeric(difftime(dropoff, pickup, units = "mins")),
    hour     = hour(pickup),
    rush_hour = (hour >= 6 & hour < 9) | (hour >= 16 & hour < 19)
  ) %>%
  filter(trip_duration > 0,
         trip_distance > 0,
         fare_amount >= 0) %>%
  select(-hour, -tpep_pickup_datetime, -tpep_dropoff_datetime)

# 4. rush vs non-rush t-tests
duration_ttest <- t.test(trip_duration ~ rush_hour, data = df2)
fare_ttest     <- t.test(fare_amount   ~ rush_hour, data = df2)

# 5. regressions
mod_dur <- lm(trip_duration ~ trip_distance + rush_hour, data = df2)
mod_fare <- lm(fare_amount   ~ trip_distance + rush_hour, data = df2)

# 6. Manhattan vs others
df3 <- df2 %>% left_join(zones, by = "PULocationID") %>%
  mutate(is_manhattan = (Borough == "Manhattan"))
man_dur_ttest <- t.test(trip_duration ~ is_manhattan, data = df3)
man_fare_ttest <- t.test(fare_amount   ~ is_manhattan, data = df3)

# 7. top pickup/dropoff zones
pickup_counts  <- df2 %>% count(PULocationID)  %>% arrange(desc(n)) %>% slice_head(n = 10)
dropoff_counts <- df2 %>% count(DOLocationID) %>% arrange(desc(n)) %>% slice_head(n = 10)

# 8. assemble and save
summary_list <- list(
  file                  = infile,
  n_records             = nrow(df2),
  duration_ttest        = tidy(duration_ttest),
  fare_ttest            = tidy(fare_ttest),
  reg_duration          = tidy(mod_dur),
  reg_fare              = tidy(mod_fare),
  manhattan_duration_ttest = tidy(man_dur_ttest),
  manhattan_fare_ttest     = tidy(man_fare_ttest),
  top_pickups           = pickup_counts,
  top_dropoffs          = dropoff_counts
)

dir.create(dirname(outfile), showWarnings = FALSE, recursive = TRUE)
saveRDS(summary_list, file = outfile)
