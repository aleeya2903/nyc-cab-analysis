#!/usr/bin/env Rscript
#
# Enhanced_Analyze.R
#
# Usage:
#   Rscript Enhanced_Analyze.R <in.parquet> <out.rds> <zones.csv>

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 3) {
  stop("Usage: Rscript Enhanced_Analyze.R <in.parquet> <out.rds> <zones.csv>")
}

infile    <- args[1]
outfile   <- args[2]
zones_csv <- args[3]

library(arrow)
library(dplyr)
library(lubridate)
library(broom)

# Helper: derive output path from outfile base
make_path <- function(suffix, ext = "csv") {
  base <- sub("\\.rds$", "", outfile)
  paste0(base, "_", suffix, ".", ext)
}

# 1. load zone lookup with both PU and DO IDs for later joining
zones_pu <- read.csv(zones_csv, stringsAsFactors = FALSE) %>%
  rename(
    PULocationID   = LocationID,
    pickup_borough = Borough,
    pickup_zone    = Zone
  )

zones_do <- read.csv(zones_csv, stringsAsFactors = FALSE) %>%
  rename(
    DOLocationID     = LocationID,
    dropoff_borough  = Borough,
    dropoff_zone     = Zone
  )

# 2. read with more columns for enhanced analysis
df <- read_parquet(infile, col_select = c(
  "tpep_pickup_datetime", "tpep_dropoff_datetime",
  "trip_distance", "fare_amount", "tip_amount", "total_amount",
  "PULocationID", "DOLocationID", "passenger_count"
))

# 3. compute more temporal features and clean data
df2 <- df %>%
  mutate(
    pickup = if (inherits(tpep_pickup_datetime, "POSIXt")) {
      tpep_pickup_datetime
    } else {
      parse_date_time(tpep_pickup_datetime,
                      orders = c("ymd HMS", "ymd HM"),
                      quiet = TRUE)
    },
    dropoff = if (inherits(tpep_dropoff_datetime, "POSIXt")) {
      tpep_dropoff_datetime
    } else {
      parse_date_time(tpep_dropoff_datetime,
                      orders = c("ymd HMS", "ymd HM"),
                      quiet = TRUE)
    },
    trip_duration   = as.numeric(difftime(dropoff, pickup, units = "mins")),
    hour            = hour(pickup),
    day_of_week     = wday(pickup, label = TRUE),
    month           = month(pickup, label = TRUE),
    rush_hour       = (hour >= 6 & hour < 10) | (hour >= 16 & hour < 20),
    weekend         = day_of_week %in% c("Sat", "Sun"),
    cost_per_minute = fare_amount / trip_duration,
    cost_per_mile   = fare_amount / trip_distance,
    tip_percentage  = (tip_amount / fare_amount) * 100
  ) %>%
  filter(
    !is.na(pickup), !is.na(dropoff),
    trip_duration > 0, trip_duration < 180,
    trip_distance > 0, trip_distance < 100,
    fare_amount >= 0, fare_amount < 500,
    !is.na(passenger_count)
  )

# 4. Join with zone information
df3 <- df2 %>%
  left_join(zones_pu, by = "PULocationID") %>%
  left_join(zones_do, by = "DOLocationID") %>%
  mutate(
    is_manhattan_pickup  = (pickup_borough == "Manhattan"),
    is_manhattan_dropoff = (dropoff_borough == "Manhattan"),
    intra_borough        = (pickup_borough == dropoff_borough),
    to_from_manhattan    = (is_manhattan_pickup != is_manhattan_dropoff)
  )

# 5. Rush hour vs non-rush hour
duration_ttest <- t.test(trip_duration ~ rush_hour, data = df3)
fare_ttest     <- t.test(fare_amount   ~ rush_hour, data = df3)
rush_summary   <- df3 %>%
  group_by(rush_hour) %>%
  summarize(
    avg_duration        = mean(trip_duration,   na.rm = TRUE),
    avg_fare            = mean(fare_amount,     na.rm = TRUE),
    avg_distance        = mean(trip_distance,   na.rm = TRUE),
    avg_cost_per_minute = mean(cost_per_minute, na.rm = TRUE),
    avg_cost_per_mile   = mean(cost_per_mile,   na.rm = TRUE),
    n_trips             = n()
  ) %>%
  ungroup()

# 6. Manhattan vs others
man_dur_ttest <- t.test(trip_duration         ~ is_manhattan_pickup, data = df3)
man_fare_ttest<- t.test(fare_amount           ~ is_manhattan_pickup, data = df3)
borough_summary <- df3 %>%
  group_by(pickup_borough) %>%
  summarize(
    avg_duration        = mean(trip_duration,   na.rm = TRUE),
    avg_fare            = mean(fare_amount,     na.rm = TRUE),
    avg_distance        = mean(trip_distance,   na.rm = TRUE),
    avg_cost_per_minute = mean(cost_per_minute, na.rm = TRUE),
    avg_cost_per_mile   = mean(cost_per_mile,   na.rm = TRUE),
    n_trips             = n()
  ) %>%
  ungroup()

# 7. Intra-borough vs inter-borough
intra_borough_ttest <- t.test(trip_duration ~ intra_borough, data = df3)
intra_fare_ttest    <- t.test(fare_amount   ~ intra_borough, data = df3)

# 8. Weekend vs weekday
weekend_dur_ttest <- t.test(trip_duration ~ weekend, data = df3)
weekend_fare_ttest<- t.test(fare_amount   ~ weekend, data = df3)

# 9. Regression models
# 9. Regression models
mod_dur  <- lm(trip_duration  ~ trip_distance + rush_hour + weekend + passenger_count,
               data = df3)
mod_fare <- lm(fare_amount    ~ trip_distance + rush_hour + weekend + passenger_count,
               data = df3)

# only keep rows where tip_percentage is finite, so lm() won’t choke on Inf/NaN
df3_tip <- df3 %>% 
  filter(is.finite(tip_percentage))

mod_tip  <- lm(tip_percentage ~ trip_duration + trip_distance + rush_hour + weekend,
               data = df3_tip)

# 10. Top locations with names
pickup_counts  <- df3 %>%
  count(PULocationID, pickup_zone, pickup_borough, name = "count") %>%
  arrange(desc(count)) %>%
  slice_head(n = 20)

dropoff_counts <- df3 %>%
  count(DOLocationID, dropoff_zone, dropoff_borough, name = "count") %>%
  arrange(desc(count)) %>%
  slice_head(n = 20)

# 11. Hourly patterns
hourly_patterns <- df3 %>%
  group_by(hour) %>%
  summarize(
    avg_duration = mean(trip_duration, na.rm = TRUE),
    avg_fare     = mean(fare_amount,   na.rm = TRUE),
    avg_distance = mean(trip_distance, na.rm = TRUE),
    n_trips      = n()
  ) %>%
  ungroup()

# 12. Day of week patterns
daily_patterns <- df3 %>%
  group_by(day_of_week) %>%
  summarize(
    avg_duration = mean(trip_duration, na.rm = TRUE),
    avg_fare     = mean(fare_amount,   na.rm = TRUE),
    avg_distance = mean(trip_distance, na.rm = TRUE),
    n_trips      = n()
  ) %>%
  ungroup()

# 13. WRITE OUTPUT FILES
dir.create(dirname(outfile), showWarnings = FALSE, recursive = TRUE)

# Basic summary statistics
basic_stats <- data.frame(
  metric = c("Total Trips", "Average Duration", "Average Fare", "Average Distance",
             "Average Passengers", "Rush Hour Trips %", "Weekend Trips %"),
  value  = c(
    nrow(df3),
    mean(df3$trip_duration,   na.rm = TRUE),
    mean(df3$fare_amount,     na.rm = TRUE),
    mean(df3$trip_distance,   na.rm = TRUE),
    mean(df3$passenger_count, na.rm = TRUE),
    mean(df3$rush_hour,       na.rm = TRUE) * 100,
    mean(df3$weekend,         na.rm = TRUE) * 100
  )
)
write.csv(basic_stats,    file = make_path("summary"),        row.names = FALSE)

# t-tests
tests <- bind_rows(
  rush_hour_duration     = tidy(duration_ttest),
  rush_hour_fare         = tidy(fare_ttest),
  manhattan_duration     = tidy(man_dur_ttest),
  manhattan_fare         = tidy(man_fare_ttest),
  intra_borough_duration = tidy(intra_borough_ttest),
  intra_borough_fare     = tidy(intra_fare_ttest),
  weekend_duration       = tidy(weekend_dur_ttest),
  weekend_fare           = tidy(weekend_fare_ttest),
  .id = "test"
) %>% arrange(p.value)
write.csv(tests, file = make_path("tests"), row.names = FALSE)

# Regression summaries
regs <- bind_rows(
  duration_model = tidy(mod_dur),
  fare_model     = tidy(mod_fare),
  tip_model      = tidy(mod_tip),
  .id = "model"
) %>% arrange(p.value)
write.csv(regs, file = make_path("regs"), row.names = FALSE)

# Counts and patterns
write.csv(pickup_counts,  file = make_path("top_pickups"),    row.names = FALSE)
write.csv(dropoff_counts, file = make_path("top_dropoffs"),   row.names = FALSE)
write.csv(hourly_patterns,file = make_path("hourly_patterns"),row.names = FALSE)
write.csv(daily_patterns, file = make_path("daily_patterns"), row.names = FALSE)
write.csv(rush_summary,   file = make_path("rush_summary"),   row.names = FALSE)
write.csv(borough_summary,file = make_path("borough_summary"),row.names = FALSE)

# Zone-level analysis
zone_summary <- df3 %>%
  group_by(pickup_zone, pickup_borough) %>%
  summarize(
    avg_duration = mean(trip_duration, na.rm = TRUE),
    avg_fare     = mean(fare_amount,    na.rm = TRUE),
    avg_distance = mean(trip_distance,  na.rm = TRUE),
    trips_count  = n()
  ) %>%
  ungroup() %>%
  arrange(desc(trips_count))

zone_hourly <- df3 %>%
  group_by(pickup_zone, hour) %>%
  summarize(
    trips_count  = n(),
    avg_duration = mean(trip_duration, na.rm = TRUE)
  ) %>%
  ungroup()

write.csv(zone_summary, file = make_path("zone_summary"), row.names = FALSE)
write.csv(zone_hourly,  file = make_path("zone_hourly"),   row.names = FALSE)

# Save RDS
summary_list <- list(
  file              = infile,
  n_records         = nrow(df3),
  rush_hour_tests   = list(duration = duration_ttest, fare = fare_ttest),
  manhattan_tests   = list(duration = man_dur_ttest, fare = man_fare_ttest),
  regression_models = list(duration = mod_dur,    fare = mod_fare,   tip = mod_tip),
  top_locations     = list(pickups = pickup_counts, dropoffs = dropoff_counts),
  time_patterns     = list(hourly = hourly_patterns, daily = daily_patterns),
  borough_analysis  = borough_summary,
  rush_analysis     = rush_summary
)
saveRDS(summary_list, file = outfile)

cat("Analysis complete!\n")
