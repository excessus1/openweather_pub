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
from src.config.config import LATITUDE, LONGITUDE, WEATHER_DB_CONNECTION, API_KEY, TIME_MACHINE_LIMIT
from src.services.api_control import APIControl

class OpenWeatherTimemachine:
    """
    Fetches historical weather data for specific timestamps using OpenWeather's API
    and stores validated data in a database.

    Attributes:
        json_file_path (str): Path to the input JSON file containing timestamps.
        latitude (float): Latitude for the API query.
        longitude (float): Longitude for the API query.
        api_key (str): API key for OpenWeather.
        logger (APILogging): Logger for tracking operations and errors.
        db_connection_string (str): Connection string for the weather database.
        location_id (int): Identifier for the target location.
        control (APIControl): Manages API rate limiting and failure tracking.
    """

    def __init__(self, json_file_path):
        """
        Initialize the OpenWeatherTimemachine class.

        Args:
            json_file_path (str): Path to the input JSON file with timestamps.
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
        self.daily_limit = TIME_MACHINE_LIMIT  # Total API call limit per day for this script
        self.start_time = time.time()  # Track when rate-limiting minute starts
        self.db_connection_string = WEATHER_DB_CONNECTION
        self.platform = "OpenWeather"
        self.api_call_type_id = 4
        self.script_name = "openweather_timemachine_get"
        self.api_call_alt_name = "timemachine"
        self.api_call_type = 'Weather Data for Timestamp'
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
            human_timestamp (str): Date in "YYYY-MM-DD HH:MM" format.

        Returns:
            int: Corresponding UNIX timestamp.
        """
        dt = datetime.strptime(human_timestamp, "%Y-%m-%d %H:%M")
        return int(time.mktime(dt.timetuple()))

    def load_json_input(self):
        """
        Load and validate the input timestamps from the JSON file.

        Returns:
            list: A list of timestamps.

        Raises:
            SystemExit: If the file is missing or the format is invalid.
        """
        if not os.path.exists(self.json_file_path):
            error_message = f"Input file {self.json_file_path} not found"
            self.logger.log_event("Error", error_message)
            self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message)
            sys.exit(1)

        try:
            with open(self.json_file_path, 'r') as json_file:
                data = json.load(json_file)

            if not isinstance(data, list) or not all(isinstance(item, int) for item in data):
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

    def call_openweather_api(self, unix_timestamp, total_timestamps, completed_timestamps):
        """
        Make an API call to OpenWeather's TimeMachine endpoint for a specific timestamp.

        Args:
            unix_timestamp (int): The target UNIX timestamp.
            total_timestamps (int): Total number of timestamps being processed.
            completed_timestamps (int): Count of completed timestamps.

        Returns:
            tuple: (Response data as dict, API call ID).

        Raises:
            SystemExit: For critical API or server errors.
        """
        try:
            # Construct the API URL
            api_prototype = self.logger.get_api_prototype(self.platform, self.api_call_type)
            base_url = api_prototype.format(lat=self.latitude, lon=self.longitude, time=unix_timestamp, API_key=self.api_key)
            human_timestamp = self.unix_to_human(unix_timestamp)

            self.logger.log_event("Request", f"Calling API: OpenWeather timemachine for timestamp: {human_timestamp}")

            # Parameters for the API request
            params = {
                'lat': self.latitude,
                'lon': self.longitude,
                'dt': unix_timestamp,
                'appid': self.api_key,
                'units': 'metric'
            }

            # Make the API request
            response = requests.get(base_url, params=params)

            call_timestamp = int(datetime.now().timestamp())
            request_payload = base_url
            retry_count = 0
            response_code = response.status_code
            response_message = f"Successfully retrieved {human_timestamp}" if response_code == 200 else f"API call failed with status {response_code} - {response.text}"

            # Log the API call in the database
            api_call_id = self.logger.log_api_call(
                call_timestamp, self.api_call_type_id, 'API Call', request_payload,
                response_code, response_message, retry_count,
                "OpenWeather API Historical Timemachine"
            )

            # Handle successful response
            if response_code == 200:
                self.logger.log_event("Success", f"Data received for timestamp: {human_timestamp}")
                progress_message = f"Processing: {completed_timestamps} of {total_timestamps}"
                self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, progress_message, None, False)
                self.control.update_requests_made_today()
                return response.json(), api_call_id

            # Handle invalid date (400 Bad Request)
            elif response_code == 400 and "out the available range" in response.text:
                error_message = f"API Error: call failed with status {response_code}: Invalid date"
                self.logger.log_event("Warning", error_message)
                self.failed_requests += 1  # Count invalid responses
                self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'Processing', error_message, False)
                self.control.update_requests_made_today()
                return None, api_call_id

            # Critical error: 404 (Not Found)
            elif response_code == 404:
                error_message = f"API call failed with status {response_code}: Not Found (URL might be wrong)"
                self.logger.log_event("Critical Error", error_message)
                self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message, True)
                self.control.update_requests_made_today()
                sys.exit(1)  # Stop execution, set force_restart flag

            # Critical error: 403 Forbidden (Invalid API Key)
            elif response_code == 403:
                error_message = f"API call failed with status {response_code}: Forbidden (Invalid API Key)"
                self.logger.log_event("Critical Error", error_message)
                self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message, True)
                self.control.update_requests_made_today()
                sys.exit(1)  # Stop execution, set force_restart flag

            # Server-side or unhandled errors
            elif response_code >= 500:
                error_message = f"Server Error (Status: {response_code}) during API call"
                self.logger.log_event("Critical Error", error_message)
                self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message, True)
                self.control.update_requests_made_today()
                sys.exit(1)  # Stop execution, set force_restart flag

            else:
                error_message = f"API Error: Unhandled API error with status {response_code}"
                self.logger.log_event("API Error: ", error_message)
                self.failed_requests += 1  # Count as a failure
                self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'Processing', error_message, True)
                self.control.update_requests_made_today()
                return None, api_call_id

        except Exception as e:
            # Log exceptions and treat as a critical failure
            error_message = f"API Error: {str(e)}"
            self.logger.log_event("API Error", error_message)
            self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-err', error_message, True)
            self.control.update_requests_made_today()
            sys.exit(1)  # Stop execution and set force_restart flag

    def store_weather_data(self, data, api_call_id):
        """
        Store validated weather data into the database.

        Args:
            data (dict): Validated weather data ready for database insertion.
            api_call_id (int): The ID associated with the API call for tracking.

        Returns:
            int: 1 if the data was successfully inserted, 0 otherwise.

        Raises:
            psycopg2.IntegrityError: If the data is already present in the database.
            Exception: For other unexpected database errors.
        """
        try:
            conn = psycopg2.connect(self.db_connection_string)
            cursor = conn.cursor()

            # Prepare values for the insert query
            values = (
                data['dt'], data['lat'], data['lon'], data['timezone'], data['timezone_offset'],
                data['sunrise'], data['sunset'], data['temp'], data['feels_like'], data['pressure'],
                data['humidity'], data['dew_point'], data['visibility'], data['description'], data['clouds'],
                data['wind_speed'], data['wind_deg'], self.location_id
            )

            # Check if the record already exists
            cursor.execute("SELECT 1 FROM hourly_data WHERE dt = %s AND location_id = %s", (data['dt'], self.location_id))
            if cursor.fetchone():
                # Log and handle duplicate records
                self.logger.log_event("data_exists", f"Record already exists for timestamp {values[0]}")
                self.logger.log_sql_insert(api_call_id, datetime.now().timestamp(), 'failure', "Duplicate record")
                self.failed_sql_inserts += 1  # Track failed inserts
                return 0

            # Insert the weather data into the database
            cursor.execute('''
                INSERT INTO hourly_data (dt, lat, lon, tz, tzoff, sunrise, sunset, temp, feels_like, pressure, humidity, dew_point, vis, description, clouds, wind_speed, wind_deg, location_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', values)
            conn.commit()

            # Log successful insert
            self.logger.log_sql_insert(api_call_id, int(datetime.now().timestamp()), f'Successfully inserted timestamp {values[0]}', None)
            self.logger.log_event("sql_insert_success", f"Record inserted for timestamp {values[0]}")
            return 1

        except psycopg2.IntegrityError:
            # Handle and log SQL IntegrityError (e.g., duplicate records)
            conn.rollback()
            self.logger.log_event("sql_insert_failure", f"Duplicate record for timestamp {values[0]}")
            self.logger.log_sql_insert(api_call_id, datetime.now().timestamp(), 'failure', "Duplicate record")
            self.failed_sql_inserts += 1  # Track failed inserts
            return 0

        except Exception as e:
            # Handle and log other SQL exceptions
            conn.rollback()
            self.logger.log_event("sql_insert_failure", f"Failed to insert data: {e}")
            self.logger.log_sql_insert(api_call_id, datetime.now().timestamp(), 'failure', str(e))
            self.failed_sql_inserts += 1  # Track failed inserts
            return 0

        finally:
            if conn:
                cursor.close()
                conn.close()

    def extract_and_validate_weather_data(self, data, api_call_id):
        """
        Extract and validate the weather data from the API response.

        Args:
            data (dict): Raw weather data from the API response.
            api_call_id (int): The ID associated with the API call for logging.

        Returns:
            dict: Validated weather data ready for database insertion, or None if validation fails.

        Raises:
            KeyError: If critical data is missing from the response.
        """
        try:
            # Extract the first weather data point
            weather_data = data['data'][0]

            # Validate visibility (set to 0 if missing or invalid)
            visibility = weather_data.get('visibility', 0)
            if not isinstance(visibility, (int, float)):
                visibility = 0
            weather_data['visibility'] = visibility

            # Check for missing critical fields
            critical_fields = ['temp', 'feels_like', 'pressure', 'humidity', 'dt']
            missing_fields = [field for field in critical_fields if weather_data.get(field) is None]

            if missing_fields:
                error_message = f"Missing critical data: {missing_fields} for timestamp {weather_data.get('dt')}"
                self.logger.handle_sql_failure(api_call_id, error_message)
                return None

            # Validate description
            description = weather_data.get('weather', [{}])[0].get('description')
            if not description:
                error_message = f"Missing 'description' for timestamp {weather_data.get('dt')}"
                self.logger.handle_sql_failure(api_call_id, error_message)
                return None

            # Create validated data dictionary
            validated_data = {
                'dt': weather_data['dt'], 'sunrise': weather_data['sunrise'], 'sunset': weather_data['sunset'],
                'temp': weather_data['temp'], 'feels_like': weather_data['feels_like'], 'pressure': weather_data['pressure'],
                'humidity': weather_data['humidity'], 'dew_point': weather_data['dew_point'], 'visibility': weather_data['visibility'],
                'description': weather_data['weather'][0]['description'], 'clouds': weather_data['clouds'], 'wind_speed': weather_data['wind_speed'],
                'wind_deg': weather_data['wind_deg'], 'lat': data['lat'], 'lon': data['lon'], 'timezone': data['timezone'], 'timezone_offset': data['timezone_offset']
            }

            return validated_data

        except (IndexError, KeyError) as e:
            # Handle data extraction errors
            error_message = f"Data extraction failed: {str(e)}"
            self.logger.handle_sql_failure(api_call_id, error_message)
            return None

    def check_for_duplicates(self, timestamps):
        """
        Check for duplicate timestamps in the database.

        Args:
            timestamps (list): List of UNIX timestamps to check.

        Returns:
            list: Timestamps that are not duplicates.
        """
        conn = psycopg2.connect(self.db_connection_string)
        cursor = conn.cursor()

        valid_timestamps = []
        for timestamp in timestamps:
            cursor.execute("SELECT 1 FROM hourly_data WHERE dt = %s AND location_id = %s", (timestamp, self.location_id))
            if cursor.fetchone() is None:
                valid_timestamps.append(timestamp)  # Only keep timestamps that aren't duplicates

        cursor.close()
        conn.close()
        return valid_timestamps

    def run(self):
        """
        Main method for executing the script's functionality.
        - Loads and validates input timestamps.
        - Filters duplicates.
        - Processes and stores weather data for valid timestamps.
        """
        self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'started')

        # Load and validate timestamps from JSON file
        timestamps = self.load_json_input()

        # Remove duplicate timestamps already present in the database
        valid_timestamps = self.check_for_duplicates(timestamps)

        # Process each valid timestamp
        for idx, unix_timestamp in enumerate(valid_timestamps):
            #Checkfor and reset the daily limits and timers
            if(self.control.check_daily_limit_reached()):
                sys.exit(1)  # Exit the script to stop further processing

            # Enforce the rate limit
            self.control.rate_limit_check()

            # Check if failure rate exceeds threshold
            if(self.control.check_failure_rate()):
                sys.exit(1)

            # Call the OpenWeather API
            weather_data, api_call_id = self.call_openweather_api(unix_timestamp, len(valid_timestamps), idx)

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
