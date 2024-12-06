import json
import os
import sys
import psycopg2
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from src.services.openweather_timemachine import OpenWeatherTimemachine
from src.config.config import (
    WEATHER_DB_CONNECTION,
    TIME_MACHINE_BATCH_LIMIT,
    TIME_MACHINE_BATCH_DIR,
    TIME_MACHINE_HISTORY_START,
    TIME_MACHINE_HISTORY_STOP,
)

# Ensure the batch directory exists or create it
if not os.path.exists(TIME_MACHINE_BATCH_DIR):
    os.makedirs(TIME_MACHINE_BATCH_DIR)


def validate_time_range(start_time, stop_time):
    """
    Validate that the start time is more recent than the stop time.

    Args:
        start_time (datetime): Start of the time range.
        stop_time (datetime): End of the time range.

    Raises:
        ValueError: If the start time is not more recent than the stop time.
    """
    if start_time <= stop_time:
        raise ValueError("TIME_MACHINE_HISTORY_START must be more recent than TIME_MACHINE_HISTORY_STOP")


def parse_start_time(start_time_config):
    """
    Parse the start time configuration, supporting special cases like 'recent'.

    Args:
        start_time_config (str): Configured start time.

    Returns:
        datetime: Parsed start time.
    """
    if start_time_config == "0000-00-00 00:00:00":
        # Start at the previous day's 23:00:00
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        yesterday_at_23 = yesterday.replace(hour=23, minute=0, second=0, microsecond=0)
        return yesterday_at_23
    else:
        return datetime.strptime(start_time_config, '%Y-%m-%d %H:%M:%S')


def generate_missing_timestamps(conn, stop_time, start_time):
    """
    Identify missing hourly timestamps using PostgreSQL's generate_series.

    Args:
        conn (psycopg2.Connection): Database connection.
        stop_time (datetime): Start of the time range.
        start_time (datetime): End of the time range.

    Returns:
        list[int]: List of missing UNIX timestamps.
    """
    query = """
        WITH all_times AS (
            SELECT generate_series(
                %s::timestamp,
                %s::timestamp,
                '1 hour'::interval
            ) AS dt
        ),
        existing_times AS (
            SELECT dt FROM hourly_data
        )
        SELECT extract(epoch from a.dt)::bigint AS missing_dt
        FROM all_times a
        LEFT JOIN existing_times e ON extract(epoch from a.dt)::bigint = e.dt
        WHERE e.dt IS NULL;
    """
    cur = conn.cursor()
    cur.execute(query, (start_time, stop_time))
    rows = cur.fetchall()
    cur.close()

    # Convert rows to a list of UNIX timestamps
    return [int(row[0]) for row in rows]

    # Convert rows to a list of UNIX timestamps
#    return [int(datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S').timestamp()) for row in rows]

def create_batch_file(timestamps, limit):
    """
    Create a batch file containing missing timestamps in JSON format.

    Args:
        timestamps (list[int]): List of missing timestamps.
        limit (int): Maximum number of timestamps to include.

    Returns:
        str: Path to the created batch file.
    """
    batch_filename = f"{TIME_MACHINE_BATCH_DIR}/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    # Limit the list
    limited_timestamps = list(timestamps)[:limit]

    # Reverse the list
    limited_reversed_timestamps = list(reversed(timestamps))[:limit]
    #print(limited_reversed_timestamps)

    # Write the timestamps to the file
    with open(batch_filename, 'w') as f:
#        json.dump(limited_timestamps, f)
        json.dump(limited_reversed_timestamps, f)

    return batch_filename

def main():
    """
    Main execution function to identify and process missing hourly weather data.

    - Parses the start and stop time configurations.
    - Validates the time range.
    - Finds missing timestamps in the database.
    - Saves timestamps to a batch file.
    - Initializes and runs OpenWeatherTimemachine for processing.
    """

    try:
        # Parse the start and stop times from the config
        # Handle the "recent" case for TIME_MACHINE_HISTORY_START
        if TIME_MACHINE_HISTORY_START == "recent":
            now = datetime.now()
            yesterday_at_23 = now - timedelta(days=1)
            yesterday_at_23 = yesterday_at_23.replace(hour=23, minute=0, second=0, microsecond=0)
            start_time = yesterday_at_23
        else:
            # Parse the start time as provided in the configuration
            start_time = datetime.strptime(TIME_MACHINE_HISTORY_START, '%Y-%m-%d %H:%M:%S')

        # Parse the stop time
        stop_time = datetime.strptime(TIME_MACHINE_HISTORY_STOP, '%Y-%m-%d %H:%M:%S')

        # Validate the time range
        validate_time_range(start_time, stop_time)

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(WEATHER_DB_CONNECTION)

        # Generate missing timestamps
        timestamps = generate_missing_timestamps(conn, start_time, stop_time)

        # If timestamps are found, process them
        if timestamps:
            # Save the timestamps to a batch file
            batch_file = create_batch_file(timestamps, TIME_MACHINE_BATCH_LIMIT)
            print(f"Batch file created: {batch_file}")

            # Initialize OpenWeatherTimemachine with the generated batch file
            openweather = OpenWeatherTimemachine(batch_file)

            # Call the run method to process the batch
            openweather.run()
        else:
            print("No missing timestamps found in the specified range.")

        # Close the database connection
        conn.close()

    except ValueError as e:
        print(f"Configuration Error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")


if __name__ == "__main__":
    main()
