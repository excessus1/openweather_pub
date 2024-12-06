import os
import sys
import json
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from services.credential_fetch import CredentialFetch


# Define the base directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # Points to project "base" directory
DATA_DIR = BASE_DIR / 'data'
LOGS_DIR = DATA_DIR / 'logs'
BATCH_FILES_DIR = DATA_DIR / 'batch_files'

# Paths to batch directories
TIME_MACHINE_BATCH_DIR = BATCH_FILES_DIR / 'timemachine'
DAILY_SUMMARY_BATCH_DIR = BATCH_FILES_DIR / 'day_summary'

# Text log file for API calls
REQUEST_LOG_FILE = LOGS_DIR / 'openweather_api_request_log.txt'

# Initialize configuration variables
TIME_MACHINE_LIMIT = None
TIME_MACHINE_BATCH_LIMIT = None
TIME_MACHINE_HOURS_HISTORY = None
DAILY_SUMMARY_LIMIT = None
DAILY_SUMMARY_BATCH_LIMIT = None
DAILY_SUMMARY_DAYS_HISTORY = None
DAILY_OVERVIEW_LIMIT = None
DAILY_FORECAST_LIMIT = None
LATITUDE = None
LONGITUDE = None
TIME_MACHINE_HISTORY_START = None
TIME_MACHINE_HISTORY_STOP = None
DAILY_SUMMARY_HISTORY_START = None
DAILY_SUMMARY_HISTORY_STOP = None
CUSTOM_PROFILE_NAME = None

# Initialize credentials variables
LOGGING_DB_CONNECTION = None
WEATHER_DB_CONNECTION = None
WEATHER_FORECAST_DB_CONNECTION = None
API_KEY = None


def load_config(config_file="config.json"):
    """
    Load configuration from a JSON file and populate global variables.

    Args:
        config_file (str): The name of the JSON configuration file.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        JSONDecodeError: If the file is not a valid JSON format.
    """

    global TIME_MACHINE_LIMIT, TIME_MACHINE_BATCH_LIMIT, TIME_MACHINE_HOURS_HISTORY
    global DAILY_SUMMARY_LIMIT, DAILY_SUMMARY_BATCH_LIMIT, DAILY_SUMMARY_DAYS_HISTORY
    global DAILY_OVERVIEW_LIMIT, DAILY_FORECAST_LIMIT, LATITUDE, LONGITUDE
    global TIME_MACHINE_HISTORY_START, TIME_MACHINE_HISTORY_STOP
    global DAILY_SUMMARY_HISTORY_START, DAILY_SUMMARY_HISTORY_STOP
    global CUSTOM_PROFILE_NAME

    try:
        # Load the configuration file
        base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        config_path = base_dir / config_file

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file '{config_path}' not found.")

        with config_path.open() as f:
            config = json.load(f)

        # Load the selected profile name
        CUSTOM_PROFILE_NAME = config.get("selected_config", "default_profile")

        # Load the selected profile
        if CUSTOM_PROFILE_NAME == "default_profile":
            custom_profile = config.get("default_profile", {})
        else:
            custom_profile = config.get("custom_profiles", {}).get(CUSTOM_PROFILE_NAME, {})


        # Load default profile
        defaults = config.get("default_profile", {})
        time_machine_defaults = defaults.get("time_machine", {})
        daily_summary_defaults = defaults.get("daily_summary", {})
        daily_overview_defaults = defaults.get("daily_overview", {})
        daily_forecast_defaults = defaults.get("daily_forecast", {})
        location_defaults = defaults.get("location", {})
        history_defaults = defaults.get("history", {})

        # Attempt to load custom profile if specified
        custom_profile = config.get("custom_profiles", {}).get(CUSTOM_PROFILE_NAME, {})
        time_machine_custom = custom_profile.get("time_machine", {})
        daily_summary_custom = custom_profile.get("daily_summary", {})
        location_custom = custom_profile.get("location", {})
        history_custom = custom_profile.get("history", {})

        # Populate the variables, falling back to defaults
        TIME_MACHINE_LIMIT = time_machine_custom.get("limit_per_day", time_machine_defaults.get("limit_per_day"))
        TIME_MACHINE_BATCH_LIMIT = time_machine_custom.get("batch_limit", time_machine_defaults.get("batch_limit"))
        TIME_MACHINE_HOURS_HISTORY = time_machine_defaults.get("hours_history")

        DAILY_SUMMARY_LIMIT = daily_summary_defaults.get("limit_per_day")
        DAILY_SUMMARY_BATCH_LIMIT = daily_summary_custom.get("batch_limit", daily_summary_defaults.get("batch_limit"))
        DAILY_SUMMARY_DAYS_HISTORY = daily_summary_defaults.get("days_history")

        DAILY_OVERVIEW_LIMIT = daily_overview_defaults.get("limit_per_day")
        DAILY_FORECAST_LIMIT = daily_forecast_defaults.get("limit_per_day")

        # Load and round location values
        LATITUDE = round(location_custom.get("latitude", location_defaults.get("latitude")), 4)
        LONGITUDE = round(location_custom.get("longitude", location_defaults.get("longitude")), 4)

        TIME_MACHINE_HISTORY_START = history_custom.get("TIME_MACHINE_HISTORY_START", history_defaults.get("TIME_MACHINE_HISTORY_START"))
        TIME_MACHINE_HISTORY_STOP = history_custom.get("TIME_MACHINE_HISTORY_STOP", history_defaults.get("TIME_MACHINE_HISTORY_STOP"))
        DAILY_SUMMARY_HISTORY_START = history_custom.get("DAILY_SUMMARY_HISTORY_START", history_defaults.get("DAILY_SUMMARY_HISTORY_START"))
        DAILY_SUMMARY_HISTORY_STOP = history_custom.get("DAILY_SUMMARY_HISTORY_STOP", history_defaults.get("DAILY_SUMMARY_HISTORY_STOP"))

    except (json.JSONDecodeError, FileNotFoundError, KeyError, TypeError) as e:
        print(f"Error loading configuration: {e}")
        print("Falling back to hardcoded defaults.")
        set_defaults()


def set_defaults():
    """
    Set default values in case of error or missing configuration.
    """
    global TIME_MACHINE_LIMIT, TIME_MACHINE_BATCH_LIMIT, TIME_MACHINE_HOURS_HISTORY
    global DAILY_SUMMARY_LIMIT, DAILY_SUMMARY_BATCH_LIMIT, DAILY_SUMMARY_DAYS_HISTORY
    global DAILY_OVERVIEW_LIMIT, DAILY_FORECAST_LIMIT, LATITUDE, LONGITUDE
    global TIME_MACHINE_HISTORY_START, TIME_MACHINE_HISTORY_STOP
    global DAILY_SUMMARY_HISTORY_START, DAILY_SUMMARY_HISTORY_STOP

    TIME_MACHINE_LIMIT = 10
    TIME_MACHINE_BATCH_LIMIT = 1
    TIME_MACHINE_HOURS_HISTORY = 100
    DAILY_SUMMARY_LIMIT = 10
    DAILY_SUMMARY_BATCH_LIMIT = 1
    DAILY_SUMMARY_DAYS_HISTORY = 10
    DAILY_OVERVIEW_LIMIT = 20
    DAILY_FORECAST_LIMIT = 20
    LATITUDE = round(33.689060, 4)
    LONGITUDE = round(-78.886696, 4)
    TIME_MACHINE_HISTORY_START = "2023-02-01 00:00:00"
    TIME_MACHINE_HISTORY_STOP = "2023-01-01 00:00:00"
    DAILY_SUMMARY_HISTORY_START = "2023-01-01 00:00:00"
    DAILY_SUMMARY_HISTORY_STOP = "2022-01-01 00:00:00"

def ensure_directories_and_files():
    """
    Ensure the required directories and files exist. Create them if missing.
    """
    for directory in [DATA_DIR, LOGS_DIR, BATCH_FILES_DIR, TIME_MACHINE_BATCH_DIR, DAILY_SUMMARY_BATCH_DIR]:
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            print(f"Created directory: {directory}")

    if not REQUEST_LOG_FILE.exists():
        REQUEST_LOG_FILE.touch()
        print(f"Created file: {REQUEST_LOG_FILE}")


def fill_credentials():
    """
    Fetch and populate credential variables for databases and API access.

    Raises:
        Exception: If credential fetching fails.
    """

    global LOGGING_DB_CONNECTION, WEATHER_DB_CONNECTION, WEATHER_FORECAST_DB_CONNECTION, API_KEY

    fetcher = CredentialFetch()

    try:
        # Fetch and set logging database credentials
        logging_credentials = fetcher.api_credential_fetch('LOGGING_DB_CONNECTION')
        LOGGING_DB_CONNECTION = f"dbname={logging_credentials['dbname']} user={logging_credentials['username']} password={logging_credentials['password']} host=localhost"

        # Fetch and set weather database credentials
        weather_credentials = fetcher.api_credential_fetch('WEATHER_DB_CONNECTION')
        WEATHER_DB_CONNECTION = f"dbname={weather_credentials['dbname']} user={weather_credentials['username']} password={weather_credentials['password']} host=localhost"

        # Fetch and set forecast database credentials
        forecast_credentials = fetcher.api_credential_fetch('WEATHER_FORECAST_DB_CONNECTION')
        WEATHER_FORECAST_DB_CONNECTION = f"dbname={forecast_credentials['dbname']} user={forecast_credentials['username']} password={forecast_credentials['password']} host=localhost"

        # Fetch and set OpenWeather API Key
        api_credentials = fetcher.api_credential_fetch('ow_api_token_excessus1')
        API_KEY = api_credentials['password']

    except Exception as e:
        print(f"Error: {e}")


# Ensure directories and files when the script is imported
ensure_directories_and_files()

# Set the safety configuration values
set_defaults()

# Load configuration when the script is imported
load_config()

# Prepare Database and API credentials for the scripts
fill_credentials()
