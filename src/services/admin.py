import psycopg2
from cryptography.fernet import Fernet
import os

# Dynamically get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the paths to the credentials and secret key files
CREDENTIALS_FILE = os.path.join(script_dir, "log.enc")  # Encrypted credentials file
SECRET_KEY_FILE = os.path.join(script_dir, "logs.enc")  # Encryption key file

def get_api_logging_connection():
    """
    Retrieve connection string for the api_logging database from an encrypted file.

    This function ensures both the secret key and encrypted credentials are present,
    reads them, decrypts the credentials using the secret key, and returns the decrypted connection string.

    Returns:
        str: Decrypted database connection string.

    Raises:
        FileNotFoundError: If either the credentials file or the secret key file is missing.
        RuntimeError: If decryption fails due to incorrect key or other errors.
    """
    # Read the secret key for decryption
    if not (os.path.exists(CREDENTIALS_FILE) and os.path.exists(SECRET_KEY_FILE)):
        raise FileNotFoundError(
            f"Required files not found: "
            f"{CREDENTIALS_FILE if not os.path.exists(CREDENTIALS_FILE) else ''} "
            f"{SECRET_KEY_FILE if not os.path.exists(SECRET_KEY_FILE) else ''}".strip()
        )

    try:
        # Read the secret key
        with open(SECRET_KEY_FILE, 'rb') as key_file:
            secret_key = key_file.read()
            api_cipher = Fernet(secret_key)

        # Read and decrypt the encrypted credentials
        with open(CREDENTIALS_FILE, 'rb') as credentials_file:
            encrypted_data = credentials_file.read()
            decrypted_data = api_cipher.decrypt(encrypted_data).decode()
            return decrypted_data  # Decrypted connection string
    except Exception as e:
        raise RuntimeError(f"Failed to decrypt credentials: {e}")

def prompt_for_key():
    """
    Prompt the user to input a base64-encoded secret key and validates it.

    Returns:
        Fernet: A Fernet encryption object initialized with the provided key.
    """

    user_key = input("Enter your secret key (base64-encoded): ").strip()
    try:
        return Fernet(user_key.encode())
    except Exception as e:
        print(f"Invalid key or error: {e}")
        quit()


# Global encryption object for credentials
credentials_cipher = prompt_for_key()


def get_db_connection():
    """
    Establish a connection to the api_logging database using the decrypted connection string.

    Returns:
        psycopg2.extensions.connection: A PostgreSQL connection object.
        None: If the connection attempt fails.
    """

    try:
        connect_string = get_api_logging_connection()
        conn = psycopg2.connect(connect_string)
        return conn
    except psycopg2.Error as e:
        print(f"Failed to connect to the database: {e}")
        return None

def show_credentials(conn):
    """
    Retrieve and display non-sensitive credential metadata from the database.

    Args:
        conn (psycopg2.extensions.connection): Active database connection.
    """

    cursor = conn.cursor()
    cursor.execute("SELECT key_name, target_service, notes FROM credentials")
    rows = cursor.fetchall()
    print("\nAvailable Credentials:")
    print("-" * 50)
    for row in rows:
        print(f"Key Name: {row[0]}, Target Service: {row[1]}, Notes: {row[2]}")
    print("-" * 50)

def add_credential(conn):
    """
    Add a new credential to the database with encrypted sensitive data.

    Args:
        conn (psycopg2.extensions.connection): Active database connection.
    """

    cursor = conn.cursor()

    key_name = input("Enter a unique key name: ")
    username = input("Enter username: ")
    password = input("Enter password: ")
    dbname = input("Enter database name (if applicable): ")
    target_service = input("Enter the target service or script (optional): ")
    notes = input("Add any additional notes (optional): ")

    # Encrypt sensitive fields
    encrypted_username = credentials_cipher.encrypt(username.encode()).decode()
    encrypted_password = credentials_cipher.encrypt(password.encode()).decode()

    # Insert the new credential
    cursor.execute(
        "INSERT INTO credentials (key_name, username, password, dbname, target_service, notes) VALUES (%s, %s, %s, %s, %s, %s)",
        (key_name, encrypted_username, encrypted_password, dbname, target_service, notes)
    )
    conn.commit()
    print(f"Credential '{key_name}' added successfully!")

def update_credential(conn):
    """
    Update an existing credential in the database.

    Prompts the user to specify the key name of the credential to update,
    the field to modify, and the new value for that field.

    Args:
        conn: A psycopg2 connection object to the database.

    Raises:
        ValueError: If the specified field is invalid.
    """
    cursor = conn.cursor()

    key_name = input("Enter the key name of the credential to update: ")
    field = input("Enter the field to update (username/password/target_service/notes/dbname): ").lower()

    if field not in ["username", "password", "target_service", "notes", "dbname"]:
        print("Invalid field.")
        return

    new_value = input(f"Enter the new value for {field}: ")
    if field in ["username", "password"]:
        new_value = credentials_cipher.encrypt(new_value.encode()).decode()

    cursor.execute(f"UPDATE credentials SET {field} = %s WHERE key_name = %s", (new_value, key_name))
    conn.commit()
    print(f"Credential '{key_name}' updated successfully!")

def remove_credential(conn):
    """
    Remove a credential from the database.

    Prompts the user to specify the key name of the credential to remove,
    then deletes the associated record.

    Args:
        conn: A psycopg2 connection object to the database.

    Raises:
        psycopg2.Error: If the delete operation fails.
    """
    cursor = conn.cursor()

    key_name = input("Enter the key name of the credential to remove: ")
    cursor.execute("DELETE FROM credentials WHERE key_name = %s", (key_name,))
    conn.commit()
    print(f"Credential '{key_name}' removed successfully!")

def fetch_credential(key_name):
    """
    Retrieve a credential by its key name from the database.

    Fetches the `username`, `password`, and `dbname` fields associated with
    the specified key name. Decrypts sensitive fields before returning.

    Args:
        key_name (str): The unique identifier for the credential.

    Returns:
        tuple: A tuple containing:
            - decrypted_username (str): The decrypted username.
            - decrypted_password (str): The decrypted password.
            - dbname (str): The associated database name.

    Raises:
        ValueError: If the key name is not found in the database.
        RuntimeError: If an error occurs during the database operation.
    """
    connection_string = get_api_logging_connection()

    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT username, password, dbname FROM credentials WHERE key_name = %s", (key_name,))
        result = cursor.fetchone()
        conn.close()

        if result:
            decrypted_username = credentials_cipher.decrypt(result[0].encode()).decode()
            decrypted_password = credentials_cipher.decrypt(result[1].encode()).decode()
            dbname = result[2]
            return decrypted_username, decrypted_password, dbname
        else:
            raise ValueError(f"Credential '{key_name}' not found.")
    except Exception as e:
        raise RuntimeError(f"Error fetching credentials: {e}")


def admin_menu(conn):
    """
    Display and manage the main admin menu for credential operations.

    Provides options for viewing, adding, updating, and removing credentials 
    stored in the database. Allows the user to exit the menu when done.

    Args:
        conn: A psycopg2 connection object to the database.

    Raises:
        Any exceptions raised by the underlying operations (e.g., database errors)
        will propagate.
    """
    while True:
        print("\nAdmin Menu:")
        print("1. Show Credentials")
        print("2. Add Credential")
        print("3. Update Credential")
        print("4. Remove Credential")
        print("5. Exit")
        choice = input("Select an option: ")

        if choice == "1":
            show_credentials(conn)
        elif choice == "2":
            add_credential(conn)
        elif choice == "3":
            update_credential(conn)
        elif choice == "4":
            remove_credential(conn)
        elif choice == "5":
            print("Exiting admin menu.")
            break
        else:
            print("Invalid choice. Please try again.")

def main():
    """
    Main entry point of the admin script.

    Handles the initial database connection and user authentication.
    Launches the admin menu upon successful connection. Closes the 
    database connection upon exit.

    Flow:
        1. Greet the user.
        2. Attempt to connect to the database.
        3. Display the admin menu if authentication is successful.
        4. Exit gracefully and close the database connection.

    Raises:
        RuntimeError: If the database connection fails.
    """
    print("Welcome to the Admin Script.")
    # Uncomment for future use:
#    username = input("Enter your database username: ")
#    password = input("Enter your database password: ")

    conn = get_db_connection()
    if conn:
        print("Authentication successful!")
        admin_menu(conn)
        conn.close()
    else:
        print("Authentication failed. Exiting.")

if __name__ == "__main__":
    main()
