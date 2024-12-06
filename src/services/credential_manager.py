import os
import psycopg2
from cryptography.fernet import Fernet


class CredentialManager:
    """
    Handles credential encryption, decryption, and database operations.

    Attributes:
        logs_file (str): Path to the secret key file for unlocking log file.
        log_file (str): Path to the encrypted log file containing credentials.
        db_key_file (str): Path to the database encryption key file.
        log_cipher (Fernet): Cipher for decrypting the log file (lazy-loaded).
        db_cipher (Fernet): Cipher for decrypting database fields (lazy-loaded).
    """

    def __init__(self, logs_file='logs.enc', log_file='log.enc', db_key_file='base.enc'):
        """
        Initialize the CredentialManager with file paths for keys and encrypted data.

        Args:
            logs_file (str): Path to the logs encryption key file.
            log_file (str): Path to the encrypted log file.
            db_key_file (str): Path to the database encryption key file.
        """
        self.logs_file = logs_file    # Secret key file for unlocking log file
        self.log_file = log_file      # Encrypted log file
        self.db_key_file = db_key_file  # Secret key file for database fields
        self.log_cipher = None        # Lazy-loaded cipher for log file decryption
        self.db_cipher = None         # Lazy-loaded cipher for database decryption

    def _initialize_cipher(self, key_file):
        """
        Initialize a Fernet cipher using a key file.

        Args:
            key_file (str): Path to the key file.

        Returns:
            Fernet: A Fernet cipher object.

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
        Ensure the log cipher is initialized by loading the secret key.

        Raises:
            FileNotFoundError: If the logs key file is not found.
        """
        if not self.log_cipher:
            if not os.path.exists(self.logs_file):
                raise FileNotFoundError(f"Key file not found: {self.logs_file}")
            with open(self.logs_file, 'rb') as f:
                secret_key = f.read().strip()
            self.log_cipher = Fernet(secret_key)

    def _ensure_db_cipher(self):
        """
        Ensure the database cipher is initialized by loading the database key file.
        """
        if not self.db_cipher:
            self.db_cipher = self._initialize_cipher(self.db_key_file)

    def get_api_logging_connection(self):
        """
        Decrypt the log file and retrieve the database connection string.

        Returns:
            str: Decrypted database connection string.

        Raises:
            FileNotFoundError: If the encrypted log file does not exist.
            RuntimeError: If decryption of the log file fails.
        """
        self._ensure_log_cipher()

        if not os.path.exists(self.log_file):
            raise FileNotFoundError(f"Encrypted log file not found: {self.log_file}")

        try:
            # Read and decrypt the encrypted log file
            with open(self.log_file, 'rb') as f:
                encrypted_data = f.read()
            return self.log_cipher.decrypt(encrypted_data).decode()
        except Exception as e:
            raise RuntimeError(f"Failed to decrypt `log.enc`: {e}")

    def get_db_connection(self):
        """
        Establish a connection to the credentials database.

        Returns:
            psycopg2.extensions.connection: Active database connection.

        Raises:
            RuntimeError: If connecting to the database fails.
        """
        connection_string = self.get_api_logging_connection()
        try:
            return psycopg2.connect(connection_string)
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to connect to the database: {e}")

    def fetch_credential(self, key_name):
        """
        Fetch and decrypt a credential by its key name.

        Args:
            key_name (str): The unique key name for the credential.

        Returns:
            tuple: Decrypted username, password, and database name.

        Raises:
            ValueError: If the credential with the specified key name is not found.
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            # Query for the credential matching the key name
            cursor.execute("SELECT username, password, dbname FROM credentials WHERE key_name = %s", (key_name,))
            result = cursor.fetchone()

            if result:
                # Decrypt sensitive fields
                self._ensure_db_cipher()
                decrypted_username = self.db_cipher.decrypt(result[0].encode()).decode()
                decrypted_password = self.db_cipher.decrypt(result[1].encode()).decode()
                dbname = result[2]
                return decrypted_username, decrypted_password, dbname
            else:
                raise ValueError(f"Credential '{key_name}' not found.")
        finally:
            cursor.close()
            conn.close()
