import datetime
import psycopg2
import os
from src.config.config import REQUEST_LOG_FILE, LOGGING_DB_CONNECTION

class APILogging:
    """
    APILogging provides functionality to log API events and interactions
    into a file and a database for monitoring and debugging purposes.

    Attributes:
        log_file (str): Path to the log file.
        db_connection (str): Connection string for the database.
    """

    def __init__(self, log_file=REQUEST_LOG_FILE, db_connection=LOGGING_DB_CONNECTION):
        self.log_file = log_file
        self.db_connection = db_connection
        self.ensure_log_file_exists()

    def ensure_log_file_exists(self):
        """
        Ensure the log file and its directory exist.

        If the directory or file does not exist, they are created.
        """
        log_dir = os.path.dirname(self.log_file)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)  # Create directory if it doesn't exist
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                f.write(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Log file created.\n")

    # Method to log events to a text file
    def log_event(self, event, message):
        """
        Write an event to the log file with a timestamp.

        Args:
            event (str): Name or description of the event.
            message (str): Details or context for the event.
        """
        with open(self.log_file, 'a') as f:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{timestamp} - {event}: {message}\n")

    # Method to insert tracking logs into the database
    def insert_tracking_log(self, script_name, platform, api_call_alt_name, status, stopped_reason=None, force_restart=False):
        """
        Insert or update tracking logs in the database.

        Args:
            script_name (str): Name of the script invoking the API.
            platform (str): Platform associated with the API.
            api_call_alt_name (str): Alternative name for the API call.
            status (str): Current status of the script or call.
            stopped_reason (str, optional): Reason for stopping (if applicable).
            force_restart (bool, optional): Whether a forced restart is required.
        """
        try:
            conn = psycopg2.connect(self.db_connection)
            cursor = conn.cursor()

            # Retrieve the previous status for tracking purposes
            cursor.execute("""
                SELECT status FROM api_script_tracking
                WHERE script_name = %s AND platform = %s AND api_call_alt_name = %s
                ORDER BY last_checked DESC LIMIT 1;
            """, (script_name, platform, api_call_alt_name))
            previous_status = cursor.fetchone()[0] if cursor.rowcount > 0 else None

            # Insert or update the tracking log
            query = """
                INSERT INTO api_script_tracking (script_name, platform, api_call_alt_name, status, previous_status, last_checked, requests_made_today, stopped_reason, force_restart)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (script_name, platform, api_call_alt_name)
                DO UPDATE SET status = EXCLUDED.status, previous_status = EXCLUDED.previous_status, last_checked = EXCLUDED.last_checked, stopped_reason = EXCLUDED.stopped_reason, force_restart = EXCLUDED.force_restart;
            """
            cursor.execute(query, (script_name, platform, api_call_alt_name, status, previous_status, datetime.datetime.now(), 0, stopped_reason, force_restart))
            conn.commit()

        except Exception as e:
            self.log_event("Error inserting tracking log", str(e))
        finally:
            if conn:
                cursor.close()
                conn.close()

    # Method to log API call details into the database
    def log_api_call(self, call_timestamp, api_call_type_id, call_event, request_payload, response_code, response_message, retry_count, call_log_message):
        """
        Log details of an API call into the database.

        Args:
            call_timestamp (datetime): Timestamp of the API call.
            api_call_type_id (int): ID representing the type of API call.
            call_event (str): Event description for the call.
            request_payload (str): Payload sent in the request.
            response_code (int): HTTP response code.
            response_message (str): Message or details of the response.
            retry_count (int): Number of retries attempted.
            call_log_message (str): Log message for the call.

        Returns:
            int: The ID of the logged API call, or None if an error occurred.
        """
        try:
            conn = psycopg2.connect(self.db_connection)
            cursor = conn.cursor()

            query = """
                INSERT INTO api_calls (call_timestamp, api_call_type_id, call_event, request_payload, response_code, response_message, retry_count, call_log_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING api_call_id;
            """
            cursor.execute(query, (call_timestamp, api_call_type_id, call_event, request_payload, response_code, response_message, retry_count, call_log_message))
            api_call_id = cursor.fetchone()[0]
            conn.commit()

            return api_call_id

        except Exception as e:
            self.log_event("Logging API Database Error", str(e))
            return None
        finally:
            if conn:
                cursor.close()
                conn.close()

    # Method to look up API prototypes from api_call_types
    def get_api_prototype(self, platform, api_call_type):
        """
        Retrieve the prototype structure for a given API call type.

        Args:
            platform (str): Platform name associated with the API.
            api_call_type (str): Type of API call.

        Returns:
            str: Prototype of the API call.

        Raises:
            ValueError: If no prototype is found for the provided platform and type.
        """
        try:
            conn = psycopg2.connect(self.db_connection)
            cursor = conn.cursor()

            query = """
                SELECT api_call_prototype
                FROM api_call_types
                WHERE platform = %s AND api_call_type = %s;
            """
            cursor.execute(query, (platform, api_call_type))
            result = cursor.fetchone()

            if result:
                return result[0]
            else:
                raise ValueError(f"No API prototype found for {platform} - {api_call_type}")

        except Exception as e:
            self.log_event("Database Error", str(e))
            raise
        finally:
            if conn:
                cursor.close()
                conn.close()

    # Method to handle SQL failures and log both to file and database
    def handle_sql_failure(self, api_call_id, error_message):
        """
        Log SQL failures both to a file and the SQL handling table.

        Args:
            api_call_id (int): ID of the API call that caused the failure.
            error_message (str): Description of the error.
        """
        self.log_event("data_validation_failure", error_message)
        self.log_sql_insert(api_call_id, datetime.datetime.now().timestamp(), 'failure', error_message)

    # Method to log SQL insert result into sql_handling
    def log_sql_insert(self, api_call_id, insert_timestamp, insert_status, error_message):
        """
        Log SQL insert results into the SQL handling table.

        Args:
            api_call_id (int): ID of the associated API call.
            insert_timestamp (float): Timestamp of the insert attempt.
            insert_status (str): Status of the insert attempt (e.g., 'success', 'failure').
            error_message (str): Error details (if any).
        """
        try:
            conn = psycopg2.connect(self.db_connection)
            cursor = conn.cursor()

            query = """
                INSERT INTO sql_handling (api_call_id, insert_timestamp, insert_status, error_message)
                VALUES (%s, %s, %s, %s);
            """
            cursor.execute(query, (api_call_id, insert_timestamp, insert_status, error_message))
            conn.commit()

        except Exception as e:
            self.log_event("SQL_handling Database Error", str(e))
        finally:
            if conn:
                cursor.close()
                conn.close()

