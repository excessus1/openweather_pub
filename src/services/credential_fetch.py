import os
from cryptography.fernet import Fernet
import psycopg2


class CredentialFetch:
    """
    Handles credential decryption and retrieval from the API logging database.

    Attributes:
        logs_file (str): Path to the encrypted logs file.
        log_file (str): Path to the encrypted log connection string file.
        db_key_file (str): Path to the encrypted database key file.
        log_cipher (Fernet): Fernet cipher for decrypting log files.
        db_cipher (Fernet): Fernet cipher for decrypting database credentials.
    """

    def __init__(self, logs_file='logs.enc', log_file='log.enc', db_key_file='base.enc'):
        """
        Initialize the CredentialFetch object with paths to encrypted files.

        Args:
            logs_file (str): Path to the encrypted logs file. Defaults to 'logs.enc'.
            log_file (str): Path to the encrypted log file. Defaults to 'log.enc'.
            db_key_file (str): Path to the encrypted database key file. Defaults to 'base.enc'.
        """
        # Base directory where this script resides
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # Resolve absolute paths for the encrypted files
        self.logs_file = os.path.join(base_dir, logs_file)
        self.log_file = os.path.join(base_dir, log_file)
        self.db_key_file = os.path.join(base_dir, db_key_file)

        # Fernet cipher instances for decryption
        self.log_cipher = None      # Cipher for decrypting log.enc
        self.db_cipher = None       # Cipher for decrypting database entries

    def _initialize_cipher(self, key_file):
        """
        Initialize a Fernet cipher from a key file.

        Args:
            key_file (str): Path to the key file.

        Returns:
            Fernet: A Fernet cipher instance for decryption.

        Raises:
            FileNotFoundError: If the key file does not exist.
        """
        if not os.path.exists(key_file):
            raise FileNotFoundError(f"Key file not found: {key_file}")
        with open(key_file, 'rb') as f:
            key = f.read().strip()
        return Fernet(key)

    def _ensure_log_cipher(self):
        """
        Ensure the cipher for `log.enc` is initialized.

        Initializes the `log_cipher` attribute if it has not been set yet.
        """
        if not self.log_cipher:
            self.log_cipher = self._initialize_cipher(self.logs_file)

    def _ensure_db_cipher(self):
        """
        Ensure the cipher for decrypting database credentials is initialized.

        Initializes the `db_cipher` attribute if it has not been set yet.
        """
        if not self.db_cipher:
            self.db_cipher = self._initialize_cipher(self.db_key_file)

    def _get_api_logging_connection_string(self):
        """
        Decrypt `log.enc` and return the connection string for api_logging.

        Returns:
            str: Decrypted connection string.

        Raises:
            FileNotFoundError: If the encrypted log file is missing.
            RuntimeError: If decryption fails for any reason.
        """
        self._ensure_log_cipher()
        if not os.path.exists(self.log_file):
            raise FileNotFoundError(f"Encrypted log file not found: {self.log_file}")

        try:
            with open(self.log_file, 'rb') as f:
                encrypted_data = f.read()
            return self.log_cipher.decrypt(encrypted_data).decode()
        except Exception as e:
            raise RuntimeError(f"Failed to decrypt `log.enc`: {e}")

    def _get_db_connection(self):
        """
        Establish a connection to the credentials database.

        Returns:
            psycopg2.connection: A connection to the database.

        Raises:
            RuntimeError: If the connection to the database fails.
        """
        connection_string = self._get_api_logging_connection_string()
        try:
            return psycopg2.connect(connection_string)
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to connect to the database: {e}")

    def api_credential_fetch(self, key_name):
        """
        Fetch and decrypt the credential for the given key_name.

        Args:
            key_name (str): The key name for the desired credential.

        Returns:
            dict: A dictionary containing decrypted username, password, and database name.

        Raises:
            ValueError: If the given key_name is not found in the database.
            RuntimeError: If fetching or decryption fails for any reason.
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()

        try:
            # Query for the credential matching the given key_name
            cursor.execute("SELECT username, password, dbname FROM credentials WHERE key_name = %s", (key_name,))
            result = cursor.fetchone()

            if result:
                self._ensure_db_cipher()
                # Decrypt the credential components
                decrypted_username = self.db_cipher.decrypt(result[0].encode()).decode()
                decrypted_password = self.db_cipher.decrypt(result[1].encode()).decode()
                dbname = result[2]
                return {
                    "username": decrypted_username,
                    "password": decrypted_password,
                    "dbname": dbname
                }
            else:
                raise ValueError(f"Credential '{key_name}' not found.")
        except Exception as e:
            raise RuntimeError(f"Failed to fetch credential '{key_name}': {e}")
        finally:
            # Ensure the database connection is properly closed
            cursor.close()
            conn.close()
