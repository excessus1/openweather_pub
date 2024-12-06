from flask import Flask, render_template, jsonify, make_response
import psycopg2
from datetime import datetime, timedelta
from src.config.config import WEATHER_DB_CONNECTION, LOGGING_DB_CONNECTION

# Initialize Flask application
app = Flask(__name__)

def get_status():
    """
    Fetches and compiles status data for the timemachine and daily summary scripts.

    Connects to the logging database to retrieve tracking, API, and SQL data
    for both scripts. If a query fails, default values are used.

    Returns:
        dict: A dictionary containing the status of timemachine and daily summary scripts.
    """
    conn = psycopg2.connect(LOGGING_DB_CONNECTION)
    cur = conn.cursor()

    # Default values in case of missing or failed queries
    default_tracking = (0, "Unknown", "Never")
    default_api = ("No API call logged", 0, 0)
    default_sql = ("No SQL insert", 0, 0)

    try:
        # Fetch tracking data for the timemachine script
        cur.execute("""
            SELECT requests_made_today, status, last_checked
            FROM api_script_tracking
            WHERE script_name = 'openweather_timemachine_get'
            LIMIT 1;
        """)
        timemachine_tracking_data = cur.fetchone() or default_tracking

        # Fetch tracking data for the daily summary script
        cur.execute("""
            SELECT requests_made_today, status, last_checked
            FROM api_script_tracking
            WHERE script_name = 'openweather_summary_get'
            LIMIT 1;
        """)
        summary_tracking_data = cur.fetchone() or default_tracking

        # Fetch the last API call for the timemachine script
        cur.execute("""
            SELECT response_message, call_timestamp, api_call_id
            FROM api_calls
            WHERE api_call_type_id = 4
            ORDER BY call_timestamp DESC
            LIMIT 1;
        """)
        timemachine_api_data = cur.fetchone() or default_api

        # Fetch the last API call for the daily summary script
        cur.execute("""
            SELECT response_message, call_timestamp, api_call_id
            FROM api_calls
            WHERE api_call_type_id = 2
            ORDER BY call_timestamp DESC
            LIMIT 1;
        """)
        summary_api_data = cur.fetchone() or default_api

        # Fetch the last SQL insert status for the timemachine script
        timemachine_api_call_id = timemachine_api_data[2]  # Extract api_call_id from the API data
        cur.execute("""
            SELECT insert_status, insert_timestamp
            FROM sql_handling
            WHERE api_call_id = %s
            ORDER BY insert_timestamp DESC
            LIMIT 1;
        """, (timemachine_api_call_id,))
        timemachine_sql_data = cur.fetchone() or default_sql

        # Fetch the last SQL insert status for the daily summary script
        summary_api_call_id = summary_api_data[2]  # Extract api_call_id from the API data
        cur.execute("""
            SELECT insert_status, insert_timestamp
            FROM sql_handling
            WHERE api_call_id = %s
            ORDER BY insert_timestamp DESC
            LIMIT 1;
        """, (summary_api_call_id,))
        summary_sql_data = cur.fetchone() or default_sql

    except Exception as e:
        # Log database errors (stdout for now, could be replaced by a logger)
        print(f"Error fetching data from the database: {e}")

    finally:
        # Always close the database connection
        conn.close()

    # Prepare the results for the timemachine script
    timemachine_status = {
        "requests_made_today": timemachine_tracking_data[0],
        "status": timemachine_tracking_data[1],
        "last_checked": timemachine_tracking_data[2],
        "last_api_call": timemachine_api_data[0],
        "last_api_time": datetime.utcfromtimestamp(timemachine_api_data[1]) if timemachine_api_data[1] != 0 else "No time",
        "last_sql_status": timemachine_sql_data[0],
        "last_sql_time": datetime.utcfromtimestamp(timemachine_sql_data[1]) if timemachine_sql_data[1] != 0 else "No time"
    }

    # Prepare the results for the daily summary script
    summary_status = {
        "requests_made_today": summary_tracking_data[0],
        "status": summary_tracking_data[1],
        "last_checked": summary_tracking_data[2],
        "last_api_call": summary_api_data[0],
        "last_api_time": datetime.utcfromtimestamp(summary_api_data[1]) if summary_api_data[1] != 0 else "No time",
        "last_sql_status": summary_sql_data[0],
        "last_sql_time": datetime.utcfromtimestamp(summary_sql_data[1]) if summary_sql_data[1] != 0 else "No time"
    }

    # Return the combined status for both scripts
    return {
        "timemachine": timemachine_status,
        "daily_summary": summary_status
    }


@app.route('/')
def index():
    """
    Flask route for the main index page.

    Renders the `ow_db_status.html` template, which serves as the dashboard.
    """
    return render_template('ow_db_status.html')


@app.route('/status')
def status():
    """
    Flask route to provide script status as JSON.

    Fetches script statuses using `get_status()` and serves them with appropriate
    caching headers for real-time updates.

    Returns:
        Response: JSON response containing script statuses.
    """
    status = get_status()
    response = make_response(jsonify(status))
    # Disable caching for dynamic data
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


if __name__ == "__main__":
    """
    Entry point for running the Flask application.

    The application listens on all interfaces (`0.0.0.0`) on port 5050 and uses SSL
    for secure communication.
    """
    app.run(host='0.0.0.0', port=5050, ssl_context=('/etc/ssl/flask/flask-selfsigned.crt', '/etc/ssl/flask/flask-selfsigned.key'))
