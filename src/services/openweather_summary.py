import requests
import sys
from datetime import datetime, timezone
import psycopg2
import sys
import time
import json
import os

from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.services.api_logger import APILogging
from src.config.config import LATITUDE, LONGITUDE, WEATHER_DB_CONNECTION, API_KEY, DAILY_SUMMARY_LIMIT
from src.services.api_control import APIControl

class OpenWeatherDailySummary:
    """
    Fetches and stores daily weather summary data from the OpenWeather API.

    Attributes:
        json_file_path (str): Path to the input JSON file containing timestamps.
        latitude (float): Latitude for the API call.
        longitude (float): Longitude for the API call.
        location_id (int): Identifier for the location.
        api_key (str): API key for OpenWeather.
        logger (APILogging): Logger for tracking and debugging.
        failed_requests (int): Count of failed API requests.
        failed_sql_inserts (int): Count of failed SQL insertions.
        batch_call_requests (int): Count of batch API calls made.
        daily_limit (int): Maximum allowed daily API calls.
        start_time (float): Start time for rate-limiting.
        db_connection_string (str): Database connection string.
        platform (str): API platform name (e.g., "OpenWeather").
        api_call_type_id (int): Identifier for the API call type.
        script_name (str): Name of the script invoking the API.
        api_call_alt_name (str): Alternative name for the API call.
        api_call_type (str): Type of API call (e.g., "Daily Aggregation").
        control (APIControl): Instance of APIControl for rate and failure checks.
    """

    def __init__(self, json_file_path):
        """
        Initialize the OpenWeatherDailySummary instance.

        Args:
            json_file_path (str): Path to the input JSON file containing timestamps.
        """
        self.json_file_path = json_file_path
        self.latitude = LATITUDE
        self.longitude = LONGITUDE
        self.location_id = 1
        self.api_key = API_KEY
        self.logger = APILogging()
        self.failed_requests = 0
        self.failed_sql_inserts = 0
        self.batch_call_requests = 0
        self.daily_limit = DAILY_SUMMARY_LIMIT  # Total API call limit per day for this script
        self.start_time = time.time()  # Track when rate-limiting minute starts
        self.db_connection_string = WEATHER_DB_CONNECTION
        self.platform = "OpenWeather"
        self.api_call_type_id = 2
        self.script_name = "openweather_summary_get"
        self.api_call_alt_name = "day_summary"
        self.api_call_type = 'Daily Aggregation'
        self.control = APIControl(self.api_call_type_id, self.script_name, self.platform, self.api_call_alt_name, self.daily_limit)

    # Function to convert UNIX timestamp to human-readable format (timezone-aware)
    def unix_to_human(self, unix_timestamp):
        """
        Convert a UNIX timestamp to a human-readable format.

        Args:
            unix_timestamp (int): UNIX timestamp.

        Returns:
            str: Human-readable date and time in UTC.
        """
        return datetime.fromtimestamp(unix_timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M")

    # Function to convert human-readable format to UNIX timestamp
    def human_to_unix(self, human_timestamp):
        """
        Convert a human-readable date to a UNIX timestamp.

        Args:
            human_timestamp (str): Date in "YYYY-MM-DD" format.

        Returns:
            int: Corresponding UNIX timestamp.
        """
        dt = datetime.strptime(human_timestamp, "%Y-%m-%d")
        return int(time.mktime(dt.timetuple()))


    def convert_tz_to_seconds(self, tz):
        """
        Convert a timezone offset string to seconds.

        Args:
            tz (str): Timezone offset in "+HH:MM" or "-HH:MM" format.

        Returns:
            int: Offset in seconds.
        """
        tz_sign = -1 if tz.startswith('-') else 1
        tz_hours, tz_minutes = map(int, tz[1:].split(':'))
        return tz_sign * (tz_hours * 3600 + tz_minutes * 60)

    def load_json_input(self):
        """
        Load and validate input timestamps from the JSON file.

        Returns:
            list: List of timestamps.

        Raises:
            SystemExit: If the file is not found or the format is invalid.
        """
        if not os.path.exists(self.json_file_path):
            error_message = f"Input file {self.json_file_path} not found"
            self.logger.log_event("Error", error_message)
            self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message)
            sys.exit(1)

        try:
            with open(self.json_file_path, 'r') as json_file:
                data = json.load(json_file)

            if not isinstance(data, list) or not all(isinstance(item, str) for item in data):
                error_message = f"Invalid input format: {data}"
                self.logger.log_event("Error", error_message)
                self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message)
                sys.exit(1)

            return data

        except json.JSONDecodeError:
            error_message = f"Failed to decode JSON from file {self.json_file_path}"
            self.logger.log_event("Error", error_message)
            self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message)
            sys.exit(1)


    def call_openweather_api(self, date, total_dates, completed_dates):
        """
        Make an API call to OpenWeather to fetch weather summary data for a specific date.

        Args:
            date (str): Date to fetch data for.
            total_dates (int): Total number of dates being processed.
            completed_dates (int): Count of completed dates so far.

        Returns:
            tuple: (API response data, API call ID)

        Raises:
            SystemExit: For critical errors (e.g., invalid API key, server-side errors).
        """
        try:
            # Construct the API URL
            api_prototype = self.logger.get_api_prototype(self.platform, self.api_call_type)
            base_url = api_prototype.format(lat=self.latitude, lon=self.longitude, date=date, API_key=self.api_key)

            self.logger.log_event("Request", f"Calling API: OpenWeather daily summary for timestamp: {date}")

            params = {
                'lat': self.latitude,
                'lon': self.longitude,
                'date': date,
                'appid': self.api_key,
                'units': 'metric'
            }

            max_retries = 3  # Maximum number of retries
            retry_count = 0
            call_timestamp = int(datetime.now().timestamp())
            request_payload = base_url

            for attempt in range(max_retries):
                try:
                    response = requests.get(base_url, params=params, timeout=5)  # Set a timeout of 15 seconds
                    response_code = response.status_code
                    response_message = f"Successfully retrieved summary for {date}" if response_code == 200 else f"API call failed with status {response_code} - {response.text}"

                    # Log the API call in the database
                    api_call_id = self.logger.log_api_call(
                        call_timestamp, self.api_call_type_id, 'API Call', request_payload,
                        response_code, response_message, retry_count,
                        "OpenWeather API Daily Summary"
                    )

                    # Handle successful response
                    if response_code == 200:
                        self.logger.log_event("Success", f"Data received for date: {date}")
                        progress_message = f"Processing: {completed_dates} of {total_dates}"
                        self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, progress_message, None, False)
                        self.control.update_requests_made_today()
                        return response.json(), api_call_id

                    # If 400 Bad Request (invalid date range), handle it as a known failure
                    elif response_code == 400 and "out the available range" in response.text:
                        error_message = f"API Error: call failed with status {response_code}: Invalid date"
                        self.logger.log_event("Warning", error_message)
                        self.failed_requests += 1  # Count invalid responses
                        self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'Processing', error_message, False)
                        self.control.update_requests_made_today()
                        return None, api_call_id

                    # Handle critical errors like 404 (Not Found) or 403 (Forbidden)
                    elif response_code == 404:
                        error_message = f"API call failed with status {response_code}: Not Found"
                        self.logger.log_event("Critical Error", error_message)
                        self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message, True)
                        self.control.update_requests_made_today()
                        sys.exit(1)  # Stop execution

                    elif response_code == 403:
                        error_message = f"API call failed with status {response_code}: Forbidden (Invalid API Key)"
                        self.logger.log_event("Critical Error", error_message)
                        self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message, True)
                        self.control.update_requests_made_today()
                        sys.exit(1)  # Stop execution

                    # Handle server-side or unhandled errors
                    elif response_code >= 500:
                        error_message = f"Server Error (Status: {response_code}) during API call"
                        self.logger.log_event("Critical Error", error_message)
                        self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message, True)
                        self.control.update_requests_made_today()
                        sys.exit(1)  # Stop execution

                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    if attempt < max_retries - 1:  # Retry if not the last attempt
                        self.logger.log_event("Warning", f"Error fetching data: {e}. Retrying ({retry_count}/{max_retries})...")
                        continue
                    else:
                        error_message = f"Error fetching data after {max_retries} attempts. Skipping this date: {date}"
                        self.logger.log_event("Error", error_message)
                        self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'Processing', error_message, True)
                        self.control.update_requests_made_today()
                        return None, None

        except Exception as e:
            # Log exceptions and treat as a critical failure
            error_message = f"API Error: {str(e)}"
            self.logger.log_event("API Error", error_message)
            self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message, True)
            self.control.update_requests_made_today()
            sys.exit(1)  # Stop execution

    def store_weather_data(self, data, api_call_id):
        """
        Store validated weather data into the database.

        Args:
            data (dict): Weather data to store.
            api_call_id (int): Associated API call ID.

        Returns:
            int: 1 if successful, 0 if duplicate or failure.
        """
        try:
            # Establish the database connection
            conn = psycopg2.connect(self.db_connection_string)
            cursor = conn.cursor()

            # Extract the values directly from the data dictionary
            values = (
                data.get('lat', 0.0),
                data.get('lon', 0.0),
                data.get('tzoff', 0),  # tzoff is already provided as seconds
                data.get('date', 0),  # UNIX timestamp
                data.get('units', 'metric'),
                data.get('cloud_cover_afternoon', 0.0),
                data.get('humidity_afternoon', 0.0),
                data.get('precipitation_total', 0.0),
                data.get('temperature_min', 0.0),
                data.get('temperature_max', 0.0),
                data.get('temperature_afternoon', 0.0),
                data.get('temperature_night', 0.0),
                data.get('temperature_evening', 0.0),
                data.get('temperature_morning', 0.0),
                data.get('pressure_afternoon', 0.0),
                data.get('wind_max_speed', 0.0),
                data.get('wind_max_direction', 0)
            )

            # Check for duplicate records based on date (UNIX timestamp)
            cursor.execute("SELECT 1 FROM daily_summary_data WHERE date = %s", (data.get('date', 0),))
            if cursor.fetchone():
                # Log and handle duplicate records
                self.logger.log_event("data_exists", f"Record already exists for date {data.get('date', 0)}")
                self.logger.log_sql_insert(api_call_id, datetime.now().timestamp(), 'failure', "Duplicate record")
                self.failed_sql_inserts += 1
                return 0

            # Insert the weather data into the database
            cursor.execute('''
                INSERT INTO daily_summary_data
                (lat, lon, tzoff, date, units, cloud_cover_afternoon, humidity_afternoon,
                precipitation_total, temperature_min, temperature_max, temperature_afternoon,
                temperature_night, temperature_evening, temperature_morning, pressure_afternoon,
                wind_max_speed, wind_max_direction)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', values)
            conn.commit()

            # Log successful insert
            self.logger.log_sql_insert(api_call_id, int(datetime.now().timestamp()), f"Successfully inserted data for date {data.get('date', 0)}", None)
            self.logger.log_event("sql_insert_success", f"Record inserted for date {data.get('date', 0)}")
            return 1

        except psycopg2.IntegrityError:
            # Handle and log SQL IntegrityError (e.g., duplicate records)
            conn.rollback()
            self.logger.log_event("sql_insert_failure", f"Duplicate record for date {data.get('date', 0)}")
            self.logger.log_sql_insert(api_call_id, datetime.now().timestamp(), 'failure', "Duplicate record")
            self.failed_sql_inserts += 1
            return 0

        except Exception as e:
            # Handle and log other SQL exceptions
            conn.rollback()
            self.logger.log_event("sql_insert_failure", f"Failed to insert data: {e}")
            self.logger.log_sql_insert(api_call_id, datetime.now().timestamp(), 'failure', str(e))
            self.failed_sql_inserts += 1
            return 0

        finally:
            # Always ensure the database connection is closed
            if conn:
                cursor.close()
                conn.close()

    def extract_and_validate_weather_data(self, data, api_call_id):
        """
        Extract and validate weather data from the API response.

        Args:
            data (dict): The raw weather data from the API response.
            api_call_id (int): The ID associated with the API call for logging.

        Returns:
            dict: Validated weather data ready for database insertion, or None if validation fails.
        """
        try:
            # Log the incoming data for debugging purposes
            #self.logger.log_event("Debug", f"Data to be validated: {data}")

            # Convert date to UNIX timestamp for storing in the database
            date_str = data.get('date', '1970-01-01')
            date_unix = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())

            # Extract the relevant fields from the API response
            cloud_cover_afternoon = data.get('cloud_cover', {}).get('afternoon', 0.0)
            humidity_afternoon = data.get('humidity', {}).get('afternoon', 0.0)
            precipitation_total = data.get('precipitation', {}).get('total', 0.0)
            temperature_min = data.get('temperature', {}).get('min', 0.0)
            temperature_max = data.get('temperature', {}).get('max', 0.0)
            temperature_afternoon = data.get('temperature', {}).get('afternoon', 0.0)
            temperature_night = data.get('temperature', {}).get('night', 0.0)
            temperature_evening = data.get('temperature', {}).get('evening', 0.0)
            temperature_morning = data.get('temperature', {}).get('morning', 0.0)
            pressure_afternoon = data.get('pressure', {}).get('afternoon', 0.0)
            wind_max_speed = data.get('wind', {}).get('max', {}).get('speed', 0.0)
            wind_max_direction = data.get('wind', {}).get('max', {}).get('direction', 0)

            # Check for missing critical fields by directly referencing the variables
            critical_fields = {
                'cloud_cover_afternoon': cloud_cover_afternoon,
                'humidity_afternoon': humidity_afternoon,
                'precipitation_total': precipitation_total,
                'temperature_min': temperature_min,
                'temperature_max': temperature_max,
                'pressure_afternoon': pressure_afternoon,
                'wind_max_speed': wind_max_speed
            }

            missing_fields = [field for field, value in critical_fields.items() if value is None]

            if missing_fields:
                error_message = f"Missing critical data: {missing_fields} for date {date_unix}"
                self.logger.handle_sql_failure(api_call_id, error_message)
                return None

            # Create validated data dictionary matching the database schema
            validated_data = {
                'lat': data.get('lat', 0.0),
                'lon': data.get('lon', 0.0),
                'tzoff': self.convert_tz_to_seconds(data.get('tz', '00:00')),  # Convert tz to seconds (e.g., -04:00 to -14400)
                'date': date_unix,  # Store as UNIX timestamp
                'units': data.get('units', 'metric'),
                'cloud_cover_afternoon': cloud_cover_afternoon,
                'humidity_afternoon': humidity_afternoon,
                'precipitation_total': precipitation_total,
                'temperature_min': temperature_min,
                'temperature_max': temperature_max,
                'temperature_afternoon': temperature_afternoon,
                'temperature_night': temperature_night,
                'temperature_evening': temperature_evening,
                'temperature_morning': temperature_morning,
                'pressure_afternoon': pressure_afternoon,
                'wind_max_speed': wind_max_speed,
                'wind_max_direction': wind_max_direction
            }

            return validated_data

        except Exception as e:
            # Handle unexpected exceptions and log the failure
            error_message = f"Error extracting and validating weather data: {str(e)}"
            self.logger.handle_sql_failure(api_call_id, error_message)
            return None

    def check_for_duplicates(self, timestamps):
        """
        Check for duplicate timestamps in the database and filter them out.

        Args:
            timestamps (list): List of timestamps to check.

        Returns:
            list: Timestamps that are not already present in the database.
        """
        conn = psycopg2.connect(self.db_connection_string)
        cursor = conn.cursor()

        valid_timestamps = []
        for timestamp in timestamps:
            # Check if the date (UNIX timestamp) exists in the daily_summary_data table
            cursor.execute("SELECT 1 FROM daily_summary_data WHERE date = %s", (self.human_to_unix(timestamp),))
            if cursor.fetchone() is None:
                valid_timestamps.append(timestamp)  # Only keep timestamps that aren't duplicates

        cursor.close()
        conn.close()
        return valid_timestamps

    def run(self):
        """
        Main execution method to process the input data and store validated weather summaries.
        """

        # Load and validate timestamps from JSON file
        timestamps = self.load_json_input()

        # Remove duplicate timestamps already present in the database
        valid_timestamps = self.check_for_duplicates(timestamps)
        if not valid_timestamps:  # Check if the list is empty
            self.logger.log_event("Warning", "All dates submitted exist in the database.")

        # Process each valid timestamp
        for idx, date in enumerate(valid_timestamps):
            #Check for and reset the daily limits and timers
            if(self.control.check_daily_limit_reached()):
                sys.exit(1)  # Exit the script to stop further processing

            # Enforce the rate limit
            self.control.rate_limit_check()

            # Check if failure rate exceeds threshold
            if(self.control.check_failure_rate()):
                sys.exit(1)

            # Call the OpenWeather API
            weather_data, api_call_id = self.call_openweather_api(date, len(valid_timestamps), idx)

            if weather_data and api_call_id:
                # Extract and validate the weather data
                extracted_weather_data = self.extract_and_validate_weather_data(weather_data, api_call_id)
                if extracted_weather_data:
                    # Store the validated weather data in the database
                    self.store_weather_data(extracted_weather_data, api_call_id)

            self.batch_call_requests += 1

        # Log script success if processing completes
        if self.failed_requests or self.failed_sql_inserts > 0:
            self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-warn')
        else:
            self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-succ')
        self.logger.log_event("Success", f"Script completed with {self.batch_call_requests} requests, {self.failed_requests} request failures, and {self.failed_sql_inserts} failed inserts.")

