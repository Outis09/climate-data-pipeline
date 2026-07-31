# Climate Data Pipeline

## Overview

This project is an Apache Airflow pipeline that fetches and processes weather, air quality, and climate data. This README documents the key design decisions, trade-offs, and lessons learned during development. It is a work in progress; some sections will be filled in as the project evolves.

## Architecture & Design Decisions

### Data Processing: For Loops vs. Dynamic Task Mapping

Two approaches were considered for processing the ~48,000 rows of city coordinate data.

**For loops**

- Advantages:
    - No need to create or store intermediate data chunks
    - Does not clutter the Airflow UI with multiple instances of the same task
- Disadvantages:
    - Processing 48,000 rows sequentially takes a long time
    - Using `time.sleep(0.5)` to stay within API limits adds up to hours of sleep time alone
    - Any error or retry means re-fetching data for all 48,000 lat/long pairs

**Dynamic task mapping**

- Advantages:
    - Splitting data into chunks allows tasks to run in parallel
    - Reduced total time, both from removing `time.sleep()` and from parallelism
    - Retries only re-run the specific chunks that failed, not the entire dataset
- Disadvantages:
    - Requires setting a max number of active task instances to avoid overwhelming Airflow resources
    - Chunks need to be stored as separate intermediate files to avoid overloading Airflow XCom with all 48,000 values in the metastore

**Final choice:** Dynamic task mapping

### Intermediate File Storage Format

**Parquet** was chosen over CSV for storing intermediate chunk files.

- Reasons:
    - Smaller file size, which matters given the number of intermediate files
    - Faster reads/writes than CSV
    - Preserves data types (CSV stores everything as text and infers types on read)
- Drawbacks:
    - Not human-readable
    - Required adding `pyarrow` as a dependency

## Data Variables

### Air Quality Variables

PM2.5, PM10, ozone, carbon monoxide, sulfur dioxide, and nitrogen dioxide were selected because they are five of the six criteria air pollutants defined by the [U.S. EPA](https://www.epa.gov/sites/default/files/2015-10/documents/ace3_criteria_air_pollutants.pdf).

| Variable | Aggregation | Reason |
|---|---|---|
| PM2.5 | 24-hour average | [U.S. EPA](https://www.epa.gov/sites/default/files/2015-10/documents/ace3_criteria_air_pollutants.pdf) |
| PM10 | 24-hour average | [U.S. EPA](https://www.epa.gov/sites/default/files/2015-10/documents/ace3_criteria_air_pollutants.pdf) |
| Ozone | Max of 8-hour rolling average | [U.S. EPA](https://www.epa.gov/sites/default/files/2015-10/documents/ace3_criteria_air_pollutants.pdf) |
| Carbon Monoxide | Max of 8-hour rolling average | [U.S. EPA](https://www.epa.gov/sites/default/files/2015-10/documents/ace3_criteria_air_pollutants.pdf) |
| Nitrogen Dioxide | Daily average | The EPA recommends a 1-hour average, but a daily average is used here since it captures the average of the 1-hour averages across a given day |
| Sulfur Dioxide | Max of 24-hour average | Same reasoning as Nitrogen Dioxide |

### Climate Variables

Variables were chosen from the essential climate variables (ECVs) defined by the [World Meteorological Organization](https://gcos.wmo.int/site/global-climate-observing-system-gcos/essential-climate-variables) that are available from Open-Meteo.

**Climate model:** EC-Earth3 was chosen because it is a globally recognized climate model and it returns all of the Open-Meteo climate variables needed.

## Design Trade-offs

### Joining Climate and River Discharge Data

- Drawbacks:
    - Removes the logical separation established at the source
    - A failure retrieving river discharge data would fail the task even if the climate API call succeeded
    - Increases the runtime of a single task, since it has to hit a different api to fetch data for 50 coordinates again
- Advantages:
    - Lower cost from less database storage and fewer intermediate files, especially as data accumulates over multiple years
    - Fewer intermediate files to manage
    - River discharge is considered a climate variable by the WMO
    - Fewer tasks overall, resulting in a simpler DAG

## Lessons Learned

- An early version used a for loop to iterate through each coordinate in a chunk, calling the API individually and waiting with `time.sleep(0.5)` between calls. For a chunk of 250 coordinates, this meant at least 125 seconds of wait time. This showed that Airflow's parallelism via dynamic task mapping reduces processing time compared to sequential processing, but does not solve every processing bottleneck on its own. Open-Meteo's API accepts up to 1,000 coordinates per request, which helps address the above.

## Known Limitations

- The project currently uses local storage, which is not sustainable once Airflow spins up multiple workers that cannot access each other's file systems.