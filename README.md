# 🚕 NYC Cab Activity Analysis (2015–2025)

A large-scale statistical and computational exploration of over 30GB of NYC taxi trip data, uncovering key insights on urban transportation trends, fare dynamics, and the impact of COVID-19 on rider behavior.

## 📊 Project Summary

This project investigates how taxi activity, pricing, and rider behavior evolved in New York City from 2015 to 2025. Using statistical modeling, high-throughput computing, and time series analysis, we explored:

- 📈 **Temporal Trends**: How cab usage and fares vary across hours, months, and years
- 🦠 **COVID-19 Impact**: Changes in average trip duration, fare, and demand
- 🗺️ **Borough-Level Insights**: Differences in travel patterns, distance, and fare pricing

## ⚙️ Methods

- **Dataset**: NYC Taxi & Limousine Commission trip data (30GB Parquet format)
- **Tools**: R, HTCondor (CHTC), RDS consolidation
- **Scale**: 1,388 parallel jobs × 1 core, 4GB RAM, 5GB disk
- **Output**: 9+ CSV reports + visualizations covering borough-level, temporal, and fare patterns

## 📌 Key Findings

- **Staten Island** trips spiked in distance during 2020–2021, reflecting pandemic travel behavior
- **EWR** (Newark Airport) had the highest average fares due to longer distances + surcharges
- **Rush hour trips** were consistently longer, with sharp COVID-era dips and slow recovery
- **Statistical tests** confirmed:
  - 🚨 Trips per zone dropped significantly after COVID (↓ 15,112 avg/month, p < 0.0001)
  - 💵 Average fare per trip rose significantly post-COVID (+$5.24 avg/trip, p < 0.0001)

## 📂 Output Highlights

- `trip_summary_by_zone.csv`: Avg trips, durations, and fares by zone
- `fare_vs_distance.png`: Correlation visualizations
- `rush_hour_comparison.csv`: Duration differences by time-of-day
- `t_test_results.txt`: Statistical significance of pre- vs. post-COVID metrics

## 👩‍💻 Team

- **Aleeya Mohamad Roki** (Data Wrangling & Statistical Modeling)
- Jonathan Morris
- Youngwoo Kim
- Kalynn Willis
- Nur Arsani

## 📬 Contact

For questions or collaboration, reach out to [mohamadroki@wisc.edu](mailto:mohamadroki@wisc.edu)
