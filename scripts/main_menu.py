import os
import sys
import psycopg2
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import numpy as np

from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from src.config.config import WEATHER_DB_CONNECTION, REQUEST_LOG_FILE, LOGGING_DB_CONNECTION

def clear_screen():
    """
    Clears the terminal screen.

    Works cross-platform by checking the OS type and running the appropriate clear command.
    """
    os.system('cls' if os.name == 'nt' else 'clear')

def view_locations():
    """
    Displays all locations stored in the database.

    Queries the `locations` table and prints the details of each location
    in a user-friendly format.
    """
    conn = psycopg2.connect(WEATHER_DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("SELECT id, friendly_name, lat_detail, lon_detail, zip_code FROM locations;")
    rows = cursor.fetchall()
    conn.close()

    print("\nLocations:")
    print("-" * 50)
    for row in rows:
        print(f"ID: {row[0]} | Name: {row[1]} | Lat: {row[2]} | Lon: {row[3]} | ZIP: {row[4]}")
    print("-" * 50)

def view_recent_hourly_data():
    """
    Displays the most recent hourly weather data in a human-friendly format.

    Queries the `hourly_data` table and displays the latest 5 entries,
    including conversions for units like temperature, pressure, and wind speed.
    """
    conn = psycopg2.connect(WEATHER_DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            l.friendly_name, 
            to_char(to_timestamp(h.dt), 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
            h.temp * 9/5 + 32 AS temp_fahrenheit,  -- Convert Celsius to Fahrenheit
            h.feels_like * 9/5 + 32 AS feels_like_fahrenheit,
            h.humidity,
            h.pressure * 0.02953 AS pressure_inHg,  -- Convert hPa to inHg
            h.wind_speed * 2.237 AS wind_speed_mph,  -- Convert m/s to mph
            h.wind_deg,
            h.description
        FROM hourly_data h
        JOIN locations l ON h.location_id = l.id
        ORDER BY h.dt DESC
        LIMIT 5;
    """)
    rows = cursor.fetchall()
    conn.close()

    print("\nRecent Hourly Data (Friendly View):")
    print("-" * 100)
    print(f"{'Location':<20} {'Timestamp':<20} {'Temp (°F)':<10} {'Feels Like (°F)':<15} {'Humidity (%)':<12} {'Pressure (inHg)':<15} {'Wind (mph)':<10} {'Wind Dir (°)':<12} {'Description':<20}")
    print("-" * 100)
    for row in rows:
        print(f"{row[0]:<20} {row[1]:<20} {row[2]:<10.1f} {row[3]:<15.1f} {row[4]:<12} {row[5]:<15.2f} {row[6]:<10.1f} {row[7]:<12} {row[8]:<20}")
    print("-" * 100)

def view_recent_daily_data():
    """
    Displays the most recent daily weather summaries.

    Queries the `daily_summary_data` table to show the latest entries,
    including metrics such as high/low temperatures, precipitation totals,
    and weather descriptions.
    """
    conn = psycopg2.connect(WEATHER_DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            l.friendly_name, 
            to_char(to_timestamp(d.date), 'YYYY-MM-DD') AS date,
            d.temperature_min * 9/5 + 32 AS temp_min_fahrenheit,  -- Convert Celsius to Fahrenheit
            d.temperature_max * 9/5 + 32 AS temp_max_fahrenheit,
            d.precipitation_total * 0.03937 AS precipitation_inches,  -- Convert mm to inches
            d.wind_max_speed * 2.237 AS wind_max_speed_mph,  -- Convert m/s to mph
            d.wind_max_direction,
            d.humidity_afternoon,
            d.cloud_cover_afternoon
        FROM daily_summary_data d
        JOIN locations l ON d.location_id = l.id
        ORDER BY d.date DESC
        LIMIT 5;
    """)
    rows = cursor.fetchall()
    conn.close()

    print("\nRecent Daily Summary Data (Friendly View):")
    print("-" * 120)
    print(f"{'Location':<20} {'Date':<15} {'Temp Min (°F)':<15} {'Temp Max (°F)':<15} {'Precip (in)':<12} {'Wind Max (mph)':<15} {'Wind Dir (°)':<12} {'Humidity (%)':<12} {'Cloud Cover (%)':<15}")
    print("-" * 120)
    for row in rows:
        print(f"{row[0]:<20} {row[1]:<15} {row[2]:<15.1f} {row[3]:<15.1f} {row[4]:<12.2f} {row[5]:<15.1f} {row[6]:<12} {row[7]:<12} {row[8]:<15}")
    print("-" * 120)

def tail_log(log_file):
    """
    Displays the last N lines of a specified log file, with user-specified scrollback.

    Args:
        log_file (str): Path to the log file to read.

    Returns:
        None
    """
    if not os.path.exists(log_file):
        print(f"\nLog file '{log_file}' does not exist.")
        return

    # Prompt user for the number of lines
    try:
        num_lines = int(input("\nEnter the number of scrollback lines (max 50): ").strip())
        if num_lines < 1 or num_lines > 50:
            print("Please enter a number between 1 and 50.")
            return
    except ValueError:
        print("Invalid input. Please enter a valid number.")
        return

    print(f"\nLast {num_lines} lines of '{log_file}':")
    print("-" * 50)
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
            if not lines:
                print("\nThe log file is empty.")
                return
            # Print only the requested number of lines
            for line in lines[-num_lines:]:
                print(line.strip())
    except Exception as e:
        print(f"An error occurred while reading the log file: {e}")

    print("-" * 50)
    print(f"Full log file can be found at: {log_file}")

def view_recent_logs():
    """
    Displays the most recent API logs.

    Queries the `api_calls` table to show the latest API calls, including timestamps,
    response codes, and any relevant log messages.
    """
    conn = psycopg2.connect(LOGGING_DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            to_char(to_timestamp(call_timestamp), 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
            t.platform,
            t.api_call_type,
            c.response_code,
            c.response_message,
            c.retry_count,
            c.call_log_message
        FROM api_calls c
        JOIN api_call_types t ON c.api_call_type_id = t.api_call_type_id
        ORDER BY call_timestamp DESC
        LIMIT 5;
    """)
    rows = cursor.fetchall()
    conn.close()

    print("\nRecent API Logs:")
    print("-" * 120)
    print(f"{'Timestamp':<20} {'Platform':<15} {'API Type':<20} {'Resp Code':<10} {'Resp Message':<25} {'Retries':<8} {'Log Message':<30}")
    print("-" * 120)
    for row in rows:
        print(f"{row[0]:<20} {row[1]:<15} {row[2]:<20} {row[3]:<10} {row[4]:<25} {row[5]:<8} {row[6]:<30}")
    print("-" * 120)


def view_recent_api_calls():
    """Displays the most recent logs from `api_calls`."""
    conn = psycopg2.connect(LOGGING_DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            to_char(to_timestamp(call_timestamp), 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
            t.platform,
            t.api_call_type,
            c.response_code,
            c.response_message,
            c.retry_count,
            c.call_log_message
        FROM api_calls c
        JOIN api_call_types t ON c.api_call_type_id = t.api_call_type_id
        ORDER BY call_timestamp DESC
        LIMIT 5;
    """)
    rows = cursor.fetchall()
    conn.close()

    print("\nRecent Logs from `api_calls`:")
    print("-" * 120)
    print(f"{'Timestamp':<20} {'Platform':<15} {'API Type':<20} {'Resp Code':<10} {'Resp Message':<25} {'Retries':<8} {'Log Message':<30}")
    print("-" * 120)
    for row in rows:
        print(f"{row[0]:<20} {row[1]:<15} {row[2]:<20} {row[3]:<10} {row[4]:<25} {row[5]:<8} {row[6]:<30}")
    print("-" * 120)

def view_api_script_tracking():
    """Displays all rows from `api_script_tracking`."""
    conn = psycopg2.connect(LOGGING_DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            script_name,
            platform,
            api_call_alt_name,
            status,
            to_char(last_checked, 'YYYY-MM-DD HH24:MI:SS') AS last_checked,
            requests_made_today,
            daily_limit_reached,
            force_restart,
            stopped_reason
        FROM api_script_tracking
        ORDER BY last_checked DESC;
    """)
    rows = cursor.fetchall()
    conn.close()

    print("\nAll Rows from `api_script_tracking`:")
    print("-" * 120)
    print(f"{'Script Name':<20} {'Platform':<15} {'Alt Name':<20} {'Status':<10} {'Last Checked':<20} {'Requests Today':<15} {'Daily Limit':<12} {'Force Restart':<15} {'Stopped Reason':<20}")
    print("-" * 120)
    for row in rows:
        # Replace None with "N/A"
        row = tuple(value if value is not None else "N/A" for value in row)
        print(f"{row[0]:<20} {row[1]:<15} {row[2]:<20} {row[3]:<10} {row[4]:<20} {row[5]:<15} {row[6]:<12} {row[7]:<15} {row[8]:<20}")
    print("-" * 120)

def view_api_call_types():
    """Displays all rows from `api_call_types`."""
    conn = psycopg2.connect(LOGGING_DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            api_call_type_id,
            platform,
            api_call_type,
            api_call_prototype
        FROM api_call_types
        ORDER BY api_call_type_id ASC;
    """)
    rows = cursor.fetchall()
    conn.close()

    print("\nAll Rows from `api_call_types`:")
    print("-" * 100)
    print(f"{'Type ID':<10} {'Platform':<15} {'API Call Type':<25} {'API Prototype':<50}")
    print("-" * 100)
    for row in rows:
        row = tuple(value if value is not None else "N/A" for value in row)  # Replace None with "N/A"
        print(f"{row[0]:<10} {row[1]:<15} {row[2]:<25} {row[3]:<50}")
    print("-" * 100)

def view_recent_sql_handling():
    """Displays the most recent logs from `sql_handling`."""
    conn = psycopg2.connect(LOGGING_DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            s.sql_log_id,
            s.insert_timestamp,
            s.insert_status,
            s.error_message,
            s.retry_count,
            c.call_log_message
        FROM sql_handling s
        LEFT JOIN api_calls c ON s.api_call_id = c.api_call_id
        ORDER BY s.insert_timestamp DESC
        LIMIT 5;
    """)
    rows = cursor.fetchall()
    conn.close()

    print("\nRecent Logs from `sql_handling`:")
    print("-" * 120)
    print(f"{'Log ID':<10} {'Timestamp':<20} {'Status':<15} {'Error Message':<30} {'Retries':<8} {'Call Log Message':<30}")
    print("-" * 120)
    for row in rows:
        row = tuple(value if value is not None else "N/A" for value in row)  # Replace None with "N/A"
        print(f"{row[0]:<10} {row[1]:<20} {row[2]:<15} {row[3]:<30} {row[4]:<8} {row[5]:<30}")
    print("-" * 120)

def view_location_hourly_data():
    """Displays hourly data for a specific location and date/time range."""
    conn = psycopg2.connect(WEATHER_DB_CONNECTION)
    cursor = conn.cursor()

    # Get location details
    cursor.execute("SELECT id, friendly_name FROM locations;")
    locations = cursor.fetchall()
    print("\nSelect a Location:")
    for loc in locations:
        print(f"{loc[0]}. {loc[1]}")
    location_id = int(input("Enter the location ID: ").strip())

    # Get date range
    start_date = input("Enter start date (YYYY-MM-DD): ").strip()
    end_date = input("Enter end date (YYYY-MM-DD): ").strip()

    cursor.execute(f"""
        SELECT 
            to_char(to_timestamp(dt - tzoff), 'YYYY-MM-DD HH24:MI:SS') AS local_timestamp,
            temp * 9/5 + 32 AS temp_fahrenheit,
            feels_like * 9/5 + 32 AS feels_like_fahrenheit,
            humidity,
            pressure * 0.02953 AS pressure_inHg,
            wind_speed * 2.237 AS wind_speed_mph,
            wind_deg,
            description
        FROM hourly_data
        WHERE location_id = %s 
          AND (dt) >= extract(epoch from timestamp %s)
          AND (dt - 86400) < extract(epoch from timestamp %s)
        ORDER BY dt ASC;
    """, (location_id, start_date, end_date))
    rows = cursor.fetchall()
    conn.close()

    data = {
        "temp": [row[1] for row in rows],
        "feels_like": [row[2] for row in rows],
        "humidity": [row[3] for row in rows],
        "pressure": [row[4] for row in rows],
        "wind_speed": [row[5] for row in rows],
        "wind_dir": [row[6] for row in rows],
        "description": [row[7] for row in rows],
    }
    timestamps = [row[0] for row in rows]


    print("\nHourly Data (Local Time):")
    print("-" * 100)
    print(f"{'Timestamp':<20} {'Temp (°F)':<10} {'Feels Like (°F)':<15} {'Humidity (%)':<12} {'Pressure (inHg)':<15} {'Wind (mph)':<10} {'Wind Dir (°)':<12} {'Description':<20}")
    print("-" * 100)
    for row in rows:
        print(f"{row[0]:<20} {row[1]:<10.1f} {row[2]:<15.1f} {row[3]:<12} {row[4]:<15.2f} {row[5]:<10.1f} {row[6]:<12} {row[7]:<20}")
    print("-" * 100)

    data_alias = {
        "Temperature (°F)": "temp",
        "Feels Like Temperature (°F)": "feels_like",
        "Humidity (%)": "humidity",
        "Pressure (inHg)": "pressure",
        "Wind Speed (mph)": "wind_speed",
        "Wind Direction (Radial)": "wind_dir",
        "Weather Descriptions (Word Cloud)": "description",
    }

    graph_menu(data, timestamps, data_alias)

def view_location_daily_data():
    """Displays daily data for a specific location and date range."""
    conn = psycopg2.connect(WEATHER_DB_CONNECTION)
    cursor = conn.cursor()

    # Get location details
    cursor.execute("SELECT id, friendly_name FROM locations;")
    locations = cursor.fetchall()
    print("\nSelect a Location:")
    for loc in locations:
        print(f"{loc[0]}. {loc[1]}")
    location_id = int(input("Enter the location ID: ").strip())

    # Get date range
    start_date = input("Enter start date (YYYY-MM-DD): ").strip()
    end_date = input("Enter end date (YYYY-MM-DD): ").strip()

    cursor.execute(f"""
        SELECT 
            to_char(to_timestamp(date - tzoff), 'YYYY-MM-DD') AS local_date,
            temperature_min * 9/5 + 32 AS temp_min_fahrenheit,
            temperature_max * 9/5 + 32 AS temp_max_fahrenheit,
            precipitation_total * 0.03937 AS precipitation_inches,
            wind_max_speed * 2.237 AS wind_max_speed_mph,
            wind_max_direction,
            humidity_afternoon,
            cloud_cover_afternoon
        FROM daily_summary_data
        WHERE location_id = %s 
          AND (date) >= extract(epoch from timestamp %s)
          AND (date - 86400) < extract(epoch from timestamp %s)
        ORDER BY date ASC;
    """, (location_id, start_date, end_date))
    rows = cursor.fetchall()
    conn.close()

    data = {
        "temperature_min": [row[1] for row in rows],
        "temperature_max": [row[2] for row in rows],
        "precipitation_total": [row[3] for row in rows],
        "wind_max_speed": [row[4] for row in rows],
        "wind_max_direction": [row[5] for row in rows],
        "humidity_afternoon": [row[6] for row in rows],
        "cloud_cover_afternoon": [row[7] for row in rows],
    }
    timestamps = [row[0] for row in rows]

    print("\nDaily Data (Local Time):")
    print("-" * 120)
    print(f"{'Date':<15} {'Temp Min (°F)':<15} {'Temp Max (°F)':<15} {'Precip (in)':<12} {'Wind Max (mph)':<15} {'Wind Dir (°)':<12} {'Humidity (%)':<12} {'Cloud Cover (%)':<15}")
    print("-" * 120)
    for row in rows:
        print(f"{row[0]:<15} {row[1]:<15.1f} {row[2]:<15.1f} {row[3]:<12.2f} {row[4]:<15.1f} {row[5]:<12} {row[6]:<12} {row[7]:<15}")
    print("-" * 120)

    data_alias = {
        "Minimum Temperature (°F)": "temperature_min",
        "Maximum Temperature (°F)": "temperature_max",
        "Precipitation (in)": "precipitation_total",
        "Maximum Wind Speed (mph)": "wind_max_speed",
        "Wind Direction (Radial)": "wind_max_direction",
        "Afternoon Humidity (%)": "humidity_afternoon",
        "Afternoon Cloud Cover (%)": "cloud_cover_afternoon",
    }
    graph_menu(data, timestamps, data_alias)



def plot_line_chart(timestamps, values, title):
    """Plots a line chart with time on the x-axis."""
    plt.figure(figsize=(10, 5))
    plt.plot(timestamps, values, marker="o", linestyle="-")
    plt.title(title)
    plt.xlabel("Time")
    plt.ylabel(title)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_wind_direction_radial(wind_directions):
    """Plots wind directions on a radial chart."""
    radians = np.radians(wind_directions)
    counts, bins = np.histogram(radians, bins=np.linspace(0, 2 * np.pi, 36))

    ax = plt.subplot(111, polar=True)
    ax.bar(bins[:-1], counts, width=np.diff(bins), align="edge", color="blue", edgecolor="black")
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    plt.title("Wind Direction (Radial)")
    plt.show()

def plot_word_cloud(descriptions):
    """
    Generates a word cloud visualization for weather descriptions.

    Queries the `hourly_data` and `daily_summary_data` tables for weather descriptions
    and creates a word cloud based on their frequency.
    """
    description_text = " ".join(descriptions)
    wordcloud = WordCloud(width=800, height=400, background_color="white").generate(description_text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title("Weather Descriptions (Word Cloud)")
    plt.show()

def plot_wind_rose(wind_directions, wind_speeds):
    """
    Plots a wind rose showing wind direction and speed.

    Parameters:
        wind_directions (list): List of wind directions in degrees (0–360).
        wind_speeds (list): List of corresponding wind speeds.
    """
    # Convert wind directions to radians
    radians = np.radians(wind_directions)

    # Create the wind rose as a polar scatter plot
    plt.figure(figsize=(8, 8))
    ax = plt.subplot(111, polar=True)
    scatter = ax.scatter(
        radians,
        wind_speeds,
        c=wind_speeds,
        cmap='viridis',
        alpha=0.75,
        edgecolors='black'
    )

    # Customize the plot
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_title("Wind Rose (Direction and Speed)", va="bottom")

    # Add a color bar for wind speeds
    cbar = plt.colorbar(scatter, pad=0.1)
    cbar.set_label("Wind Speed (mph)")

    # Show the plot
    plt.show()


def graph_menu(data, timestamps, data_alias):
    """
    Menu for generating graphs based on data and user selection.

    Parameters:
        data: Dictionary containing the queried data (e.g., temp, humidity).
        timestamps: List of timestamps corresponding to the data.
        data_alias: Dictionary mapping user-friendly options to data keys in `data`.
    """
    while True:
        print("\nGraphing Options:")
        for idx, (label, key) in enumerate(data_alias.items(), 1):
            print(f"{idx}. {label}")
        print(f"{len(data_alias) + 1}. Wind Rose (Direction and Speed)")
        print(f"{len(data_alias) + 2}. Exit Graphing")

        try:
            choice = int(input("Select an option: ").strip())
        except ValueError:
            print("Invalid input. Please enter a number.")
            continue

        if 1 <= choice <= len(data_alias):
            label, key = list(data_alias.items())[choice - 1]
            if (key == "wind_dir") or (key == "wind_max_direction"):
                plot_wind_direction_radial(data["wind_dir"])
            elif key == "description":
                plot_word_cloud(data["description"])
            else:
                plot_line_chart(timestamps, data[key], label)
        elif choice == len(data_alias) + 1:
            # Determine the appropriate keys for wind direction and speed
            wind_dir_key = "wind_dir" if "wind_dir" in data else "wind_max_direction"
            wind_speed_key = "wind_speed" if "wind_speed" in data else "wind_max_speed"

            # Call the wind rose function with the appropriate keys
            plot_wind_rose(data[wind_dir_key], data[wind_speed_key])
        elif choice == len(data_alias) + 2:
            print("Exiting graphing menu.")
            break
        else:
            print("Invalid choice. Please try again.")


def api_logs_submenu():
    """Submenu to view recent API logs from various tables."""
    while True:
        clear_screen()
        print("API Logs Submenu")
        print("-" * 50)
        print("1. View Recent Logs from `api_calls`")
        print("2. View All Rows from `api_script_tracking`")
        print("3. View All Rows from `api_call_types`")
        print("4. View Recent Logs from `sql_handling`")
        print("5. Tail Log File")
        print("6. Return to Main Menu")
        print("-" * 50)

        choice = input("Select an option: ").strip()

        if choice == "1":
            view_recent_api_calls()
        elif choice == "2":
            view_api_script_tracking()
        elif choice == "3":
            view_api_call_types()
        elif choice == "4":
            view_recent_sql_handling()
        elif choice == "5":
            log_file = REQUEST_LOG_FILE
            tail_log(log_file)
        elif choice == "6":
            break
        else:
            print("Invalid option. Please try again.")
        input("\nPress Enter to continue...")

def recent_data_submenu():
    """Submenu for viewing recent data."""
    while True:
        clear_screen()
        print("Recent Data Submenu")
        print("-" * 50)
        print("1. View Recent Hourly Data")
        print("2. View Recent Daily Data")
        print("3. Return to Main Menu")
        print("-" * 50)

        choice = input("Select an option: ").strip()

        if choice == "1":
            view_recent_hourly_data()
        elif choice == "2":
            view_recent_daily_data()
        elif choice == "3":
            break
        else:
            print("Invalid option. Please try again.")
        input("\nPress Enter to continue...")

def location_data_submenu():
    """Submenu for viewing location-specific data."""
    while True:
        clear_screen()
        print("Location Data Submenu")
        print("-" * 50)
        print("1. View Hourly Data for a Location")
        print("2. View Daily Data for a Location")
        print("3. Return to Main Menu")
        print("-" * 50)

        choice = input("Select an option: ").strip()

        if choice == "1":
            view_location_hourly_data()
        elif choice == "2":
            view_location_daily_data()
        elif choice == "3":
            break
        else:
            print("Invalid option. Please try again.")
        input("\nPress Enter to continue...")

def main_menu():
    """Main menu for the CLI system."""
    while True:
        clear_screen()
        print("OpenWeather Management System")
        print("-" * 50)
        print("1. View Locations")
        print("2. Recent Data Submenu")  # Moved hourly/daily data here
        print("3. API Logs Submenu")     # Moved tail log here
        print("4. Location Data Submenu")  # New submenu
        print("5. Exit")
        print("-" * 50)

        choice = input("Select an option: ").strip()

        if choice == "1":
            view_locations()
        elif choice == "2":
            recent_data_submenu()
        elif choice == "3":
            api_logs_submenu()
        elif choice == "4":
            location_data_submenu()
        elif choice == "5":
            print("Exiting...")
            break
        else:
            print("Invalid option. Please try again.")

        input("\nPress Enter to return to the menu...")

if __name__ == "__main__":
    main_menu()
