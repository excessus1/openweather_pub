import psycopg2
from psycopg2 import sql

# Placeholder SQL scripts
API_LOGGING_SQL = "/path/to/api_logging.sql"
OPENWEATHER_HISTORICAL_SQL = "/path/to/openweather_historical.sql"

def load_sql(file_path):
    """
    Load SQL content from a file.
    
    Args:
        file_path (str): Path to the SQL file.

    Returns:
        str: SQL commands as a string.
    """
    with open(file_path, 'r') as file:
        return file.read()

def create_database_and_tables(db_name, user, password, host, port, sql_file_path):
    """
    Create a PostgreSQL database and initialize tables.

    Args:
        db_name (str): Name of the database to create.
        user (str): PostgreSQL username.
        password (str): PostgreSQL password.
        host (str): Database host.
        port (int): Database port.
        sql_file_path (str): Path to the SQL file containing table definitions.
    """
    try:
        # Connect to the PostgreSQL server
        connection = psycopg2.connect(
            dbname="postgres",
            user=user,
            password=password,
            host=host,
            port=port
        )
        connection.autocommit = True
        cursor = connection.cursor()

        # Create the database
        cursor.execute(sql.SQL("CREATE DATABASE {} OWNER {};").format(
            sql.Identifier(db_name),
            sql.Identifier(user)
        ))
        print(f"Database '{db_name}' created successfully.")

        # Close the initial connection
        cursor.close()
        connection.close()

        # Connect to the newly created database to create tables
        connection = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = connection.cursor()

        # Execute the SQL file to create tables
        sql_commands = load_sql(sql_file_path)
        cursor.execute(sql_commands)
        connection.commit()
        print(f"Tables initialized in database '{db_name}'.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if connection:
            cursor.close()
            connection.close()


def main():
    """
    Main function to interactively create databases and tables.
    """
    print("PostgreSQL Database and Table Setup")
    user = input("Enter PostgreSQL username: ").strip()
    password = input("Enter PostgreSQL password: ").strip()
    host = input("Enter PostgreSQL host (default: localhost): ").strip() or "localhost"
    port = input("Enter PostgreSQL port (default: 5432): ").strip() or "5432"

    # Database and SQL file details
    setups = [
        {"db_name": "api_logging", "sql_file": API_LOGGING_SQL},
        {"db_name": "openweather_historical", "sql_file": OPENWEATHER_HISTORICAL_SQL}
    ]

    for setup in setups:
        create_database_and_tables(setup["db_name"], user, password, host, port, setup["sql_file"])


if __name__ == "__main__":
    main()
