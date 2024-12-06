import psycopg2
import time
from datetime import datetime, timedelta
import sys
from src.services.api_logger import APILogging

class APIControl:
    def __init__(self, api_call_type_id, script_name, platform, api_call_alt_name, daily_limit):
        """
        Initialize the APIControl class.

        Args:
            api_call_type_id (int): Unique identifier for the type of API call.
            script_name (str): Name of the script making the API calls.
            platform (str): The platform associated with the API calls (e.g., "OpenWeather").
            api_call_alt_name (str): Alternative name for the API call (used in tracking).
            daily_limit (int): Maximum number of API calls allowed per day.
        """
        self.api_call_type_id = api_call_type_id
        self.script_name = script_name
        self.platform = platform
        self.api_call_alt_name = api_call_alt_name
        self.daily_limit = daily_limit
        self.logger = APILogging()

    def requests_today(self):
        """
        Get the count of API requests made since 00:00 UTC today.

        Returns:
            int: The number of API calls made today.
        """
        current_utc_time = datetime.utcnow()
        reset_time_utc = current_utc_time.replace(hour=0, minute=0, second=0, microsecond=0)

        conn = psycopg2.connect(self.logger.db_connection)
        cursor = conn.cursor()

        # Query the number of API calls made since 00:00 UTC today
        query = """
            SELECT COUNT(*) 
            FROM api_calls 
            WHERE api_call_type_id = %s AND call_timestamp >= extract(epoch from %s);
        """
        cursor.execute(query, (self.api_call_type_id, reset_time_utc))

        count = cursor.fetchone()[0]  # Get the count of requests made today

        cursor.close()
        conn.close()
        
        return count

    def check_daily_limit_reached(self):
        """
        Check if the daily API call limit has been reached.

        Returns:
            bool: True if the daily limit has been reached, False otherwise.

        Side Effects:
            - Logs daily request counts and limits.
            - Updates the tracking table in the database when the daily limit is reached or reset.
        """
        requests_made_today = self.requests_today()       

        current_utc_time = datetime.utcnow()

        conn = psycopg2.connect(self.logger.db_connection)
        cursor = conn.cursor()

        # Log the real-time count of requests made today
        self.logger.log_event("Daily Request Check", f"Requests made today: {requests_made_today}")

        # Check if the daily limit has been reached
        if requests_made_today >= self.daily_limit:
            # Update the api_script_tracking to indicate the daily limit has been reached
            cursor.execute("""
                UPDATE api_script_tracking
                SET daily_limit_reached = TRUE, last_checked = %s
                WHERE script_name = %s AND platform = %s AND api_call_alt_name = %s;
            """, (current_utc_time, self.script_name, self.platform, self.api_call_alt_name))
            conn.commit()
            self.logger.insert_tracking_log(self.script_name, self.platform, self.api_call_alt_name, 'stopped-warn', 'Daily limit reached', False)
            self.logger.log_event("Daily Limit", "Daily API limit reached. No further processing for today.")
            return True #Daily limit is reached

        # If no calls have been made today (count = 0), reset the daily limit tracking
        if requests_made_today == 0:
            cursor.execute("""
                UPDATE api_script_tracking
                SET requests_made_today = 0, daily_limit_reached = FALSE, last_checked = %s
                WHERE script_name = %s AND platform = %s AND api_call_alt_name = %s;
            """, (current_utc_time, self.script_name, self.platform, self.api_call_alt_name))
            conn.commit()
            self.logger.log_event("Daily Limit Reset", "A new day has started. Daily API limit has been reset.")
        
        cursor.close()
        conn.close()
        return False  #Daily limit not reached

    def update_requests_made_today(self):
        """
        Increment the count of requests made today in the tracking table.

        Side Effects:
            - Updates the `requests_made_today` and `last_checked` fields in the database.
            - Logs the updated request count.
        """
        conn = psycopg2.connect(self.logger.db_connection)
        cursor = conn.cursor()

        # Get the current count of requests made today
        requests_made_today = self.requests_today()

        # Update the count of requests made today
        query = """
            UPDATE api_script_tracking
            SET requests_made_today = requests_made_today + 1, last_checked = %s
            WHERE script_name = %s AND platform = %s AND api_call_alt_name = %s;
        """
        current_utc_time = datetime.utcnow()  # Current time for last_checked field

        cursor.execute(query, (current_utc_time, self.script_name, self.platform, self.api_call_alt_name))
        conn.commit()

        # Log the update
        self.logger.log_event("Request Update", f"Requests made today incremented to {requests_made_today + 1}")

        cursor.close()
        conn.close()

    def rate_limit_check(self):
        """
        Enforce a rate limit based on API calls in the last 5 minutes.

        Side Effects:
            - Calculates the rate of API calls and enforces delays if needed.
            - Logs rate limit checks and enforced delays.
            - Sleeps for the calculated interval to maintain the rate limit.
        """
        conn = psycopg2.connect(self.logger.db_connection)
        cursor = conn.cursor()

        # Step 1: Calculate calls allowed in 5 minutes and allowed call interval
        calls_in_5_minutes_allowed = 300 * 0.9  # 90% of the 300 seconds
        allowed_call_interval = 300 / calls_in_5_minutes_allowed  # seconds between calls

        # Step 2: Calculate the time 5 minutes ago
        time_5_minutes_ago = datetime.utcnow() - timedelta(seconds=300)

        # Step 3: Query to find all api_call_type_id associated with self.platform from api_call_types
        query = """
            SELECT api_call_type_id
            FROM api_call_types
            WHERE platform = %s;
        """
        cursor.execute(query, (self.platform,))
        api_call_type_ids = [row[0] for row in cursor.fetchall()]  # Fetch all api_call_type_id

        # Step 4: Query to count the number of calls in the last 5 minutes for the fetched api_call_type_id values
        query = """
            SELECT COUNT(*)
            FROM api_calls
            WHERE api_call_type_id = ANY(%s) AND call_timestamp >= extract(epoch from %s);
        """
        cursor.execute(query, (api_call_type_ids, time_5_minutes_ago))
        calls_in_last_5_minutes = cursor.fetchone()[0]  # Get the count of requests in the last 5 minutes

        # Step 5: Calculate the next interval based on the number of calls and the allowed interval
        current_call_rate = calls_in_last_5_minutes / 300  # Calls per second
        next_interval = allowed_call_interval + (current_call_rate * allowed_call_interval)

        # Step 6: Log the current call rate and next interval
        self.logger.log_event("Rate Limit Check", f"Calls in the last 5 minutes: {calls_in_last_5_minutes}, Next interval: {next_interval:.2f} seconds")

        # Step 7: Sleep for the next interval to enforce the rate limit
        if next_interval > 0:
            self.logger.log_event("Rate Limit", f"Sleeping for {next_interval:.2f} seconds to enforce rate limit.")
            time.sleep(next_interval)
        else:
            self.logger.log_event("Rate Limit", "No need to sleep, the call rate is within limits.")

        cursor.close()
        conn.close()

    def check_failure_rate(self):
        """
        Check the failure rate of API calls in the last 2 minutes.

        Returns:
            bool: True if the failure rate exceeds thresholds, False otherwise.

        Failure Thresholds:
            - More than 10 failed calls in the last 2 minutes.
            - Failure rate exceeds 20% of successful calls.

        Side Effects:
            - Logs failure and success counts.
            - Logs whether thresholds have been exceeded.
        """
        conn = psycopg2.connect(self.logger.db_connection)
        cursor = conn.cursor()

        # Step 1: Calculate the time 2 minutes ago
        time_2_minutes_ago = datetime.utcnow() - timedelta(seconds=120)

        # Step 2: Query to find all api_call_type_id associated with self.platform from api_call_types
        query = """
            SELECT api_call_type_id
            FROM api_call_types
            WHERE platform = %s;
        """
        cursor.execute(query, (self.platform,))
        api_call_type_ids = [row[0] for row in cursor.fetchall()]  # Fetch all api_call_type_id

        # Step 3: Query to count the number of failed (response_code != 200) calls in the last 2 minutes
        query = """
            SELECT COUNT(*)
            FROM api_calls
            WHERE api_call_type_id = ANY(%s) AND call_timestamp >= extract(epoch from %s) AND response_code != 200;
        """
        cursor.execute(query, (api_call_type_ids, time_2_minutes_ago))
        failure_count = cursor.fetchone()[0]  # Get the count of failed calls

        # Step 4: Query to count the number of successful (response_code == 200) calls in the last 2 minutes
        query = """
            SELECT COUNT(*)
            FROM api_calls
            WHERE api_call_type_id = ANY(%s) AND call_timestamp >= extract(epoch from %s) AND response_code = 200;
        """
        cursor.execute(query, (api_call_type_ids, time_2_minutes_ago))
        success_count = cursor.fetchone()[0]  # Get the count of successful calls
        cursor.close()
        conn.close()
        
        # Step 5: Calculate failure rate and check the thresholds
        if failure_count > 10 and (success_count > 0 and (failure_count / success_count) > 0.2):
            self.logger.log_event("Failure Rate Check", f"Failures exceed threshold: {failure_count} failures in last 2 minutes.")
            return True  # Failure rate exceeds threshold
        else:
            self.logger.log_event("Failure Rate Check", f"Failures within limits: {failure_count} failures, {success_count} successes.")
            return False  # Failure rate within acceptable limits



