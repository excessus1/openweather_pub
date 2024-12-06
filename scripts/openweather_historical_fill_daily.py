import json
import os
import sys
import psycopg2
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from src.services.openweather_summary import OpenWeatherDailySummary
from src.config.config import (
    WEATHER_DB_CONNECTION,
    DAILY_SUMMARY_BATCH_LIMIT,
    DAILY_SUMMARY_BATCH_DIR,
    DAILY_SUMMARY_HISTORY_START,
    DAILY_SUMMARY_HISTORY_STOP,
)

# Ensure the batch directory exists or create it
if not os.path.exists(DAILY_SUMMARY_BATCH_DIR):
    os.makedirs(DAILY_SUMMARY_BATCH_DIR)


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
        raise ValueError("DAILY_SUMMARY_HISTORY_START must be more recent than DAILY_SUMMARY_HISTORY_STOP")


def parse_start_time(start_time_config):
    """
    Parse the start time from the configuration, handling special cases like 'recent'.

    Args:
        start_time_config (str): Configured start time.

    Returns:
        datetime: Parsed start time.
    """
    if start_time_config == "recent":
        # Start at the previous day's 23:00:00
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        yesterday_at_23 = yesterday.replace(hour=23, minute=0, second=0, microsecond=0)
        return yesterday_at_23
    else:
        return datetime.strptime(start_time_config, '%Y-%m-%d %H:%M:%S')


def generate_missing_daily_timestamps(conn, stop_time, start_time):
    """
    Find missing daily timestamps using PostgreSQL's generate_series.

    Args:
        conn (psycopg2.Connection): Database connection.
        stop_time (datetime): Start of the time range.
        start_time (datetime): End of the time range.

    Returns:
        list[str]: List of missing dates as strings in 'YYYY-MM-DD' format.
    """
    query = """
        WITH all_dates AS (
            SELECT generate_series(
                %s::timestamp,
                %s::timestamp,
                '1 day'::interval
            ) AS date
        ),
        existing_dates AS (
            SELECT to_timestamp(date) AS date FROM daily_summary_data
        )
        SELECT to_char(a.date, 'YYYY-MM-DD') AS missing_date
        FROM all_dates a
        LEFT JOIN existing_dates e ON a.date = e.date
        WHERE e.date IS NULL;
    """

    cur = conn.cursor()
    cur.execute(query, (start_time, stop_time))
    rows = cur.fetchall()
    cur.close()

    # Return the missing dates as a list of strings
    return [row[0] for row in rows]


def create_batch_file(timestamps, limit):
    """
    Save the reversed and limited timestamps to a batch file in JSON format.

    Args:
        timestamps (list[str]): List of missing timestamps.
        limit (int): Maximum number of timestamps to include in the batch file.

    Returns:
        str: Path to the created batch file.
    """
    batch_filename = f"{DAILY_SUMMARY_BATCH_DIR}/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    # Reverse and limit the list
    limited_reversed_timestamps = list(reversed(timestamps))[:limit]

    # Write the timestamps to the file
    with open(batch_filename, 'w') as f:
        json.dump(limited_reversed_timestamps, f)

    return batch_filename


def main():
    """
    Main execution function to identify and process missing daily weather data.

    - Parses the start and stop time configurations.
    - Validates the time range.
    - Finds missing daily timestamps in the database.
    - Saves timestamps to a batch file.
    - Initializes and runs OpenWeatherDailySummary for processing.
    """

    try:
        # Handle the "recent" case for DAILY_SUMMARY_HISTORY_START
        if DAILY_SUMMARY_HISTORY_START == "recent":
            now = datetime.now()
            yesterday_at_23 = now - timedelta(days=1)
            yesterday_at_23 = yesterday_at_23.replace(hour=23, minute=0, second=0, microsecond=0)
            start_time = yesterday_at_23
        else:
            # Parse the start time as provided in the configuration
            start_time = datetime.strptime(DAILY_SUMMARY_HISTORY_START, '%Y-%m-%d %H:%M:%S')

        # Parse the stop time
        stop_time = datetime.strptime(DAILY_SUMMARY_HISTORY_STOP, '%Y-%m-%d %H:%M:%S')

        # Validate the time range
        validate_time_range(start_time, stop_time)

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(WEATHER_DB_CONNECTION)

        # Generate missing daily timestamps
        timestamps = generate_missing_daily_timestamps(conn, start_time, stop_time)

        # If timestamps are found, process them
        if timestamps:
            # Save the timestamps to a batch file
            batch_file = create_batch_file(timestamps, DAILY_SUMMARY_BATCH_LIMIT)
            print(f"Batch file created: {batch_file}")

            # Initialize OpenWeatherDailySummary with the generated batch file
            openweather = OpenWeatherDailySummary(batch_file)

            # Call the run method to process the batch
            openweather.run()
        else:
            print("No missing data found in the specified range.")

        # Close the database connection
        conn.close()

    except ValueError as e:
        print(f"Configuration Error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")


if __name__ == "__main__":
    main()
