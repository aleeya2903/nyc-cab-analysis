# Visualization (Aleeya and Nur)

library(dplyr)
library(ggplot2)
library(lubridate)
library(viridis)

# test for one file
df = read_parquet('yellow_tripdata_2022-10.parquet')

df = df %>%
  mutate(
    trip_duration = as.numeric(difftime(tpep_dropoff_datetime, tpep_pickup_datetime, units = "mins")),
    rush_hour = ifelse(
      (hour(tpep_pickup_datetime) %in% 6:9) | 
      (hour(tpep_pickup_datetime) %in% 16:19),
      "Rush Hour", "Non-Rush Hour"),
    hour_of_day = hour(tpep_pickup_datetime)
  ) %>%
  filter(
    trip_duration > 0 & trip_duration < 120, 
    fare_amount > 0,                         
    trip_distance > 0          
  )

ggplot(df, aes(x = trip_duration, fill = rush_hour)) +
  geom_density(alpha = 0.6) +
  labs(title = "Trip Duration Distribution by Time of Day",
       x = "Trip Duration (minutes)",
       y = "Density",
       fill = "Time Period") +
  theme_minimal() +
  scale_fill_viridis(discrete = TRUE)

df %>%
  group_by(hour_of_day) %>%
  summarise(avg_fare = mean(fare_amount)) %>%
  ggplot(aes(x = hour_of_day, y = avg_fare)) +
  geom_line(color = "steelblue", linewidth = 1.5) +
  geom_point(color = "darkred", size = 2) +
  labs(title = "Average Fare by Hour of Day",
       x = "Hour of Day",
       y = "Average Fare ($)") +
  theme_minimal()
