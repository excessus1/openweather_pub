import os
from cryptography.fernet import Fernet


def generate_new_key():
    """
    Generate a new encryption key and return it.
    """
    secret_key = Fernet.generate_key()
    print("Generated Secret Key (Save securely):")
    print(secret_key.decode())
    return secret_key


def encrypt_message(secret_key, message):
    """
    Encrypt a message using the provided secret key.

    Args:
        secret_key (bytes): Encryption key.
        message (str): Message to encrypt.

    Returns:
        bytes: Encrypted message.
    """
    cipher = Fernet(secret_key)
    encrypted_message = cipher.encrypt(message.encode())
    return encrypted_message


def decrypt_message(secret_key, encrypted_message):
    """
    Decrypt a message using the provided secret key.

    Args:
        secret_key (bytes): Encryption key.
        encrypted_message (bytes): Encrypted message to decrypt.

    Returns:
        str: Decrypted message.
    """
    cipher = Fernet(secret_key)
    try:
        return cipher.decrypt(encrypted_message).decode()
    except Exception as e:
        print(f"Decryption failed: {e}")
        return None


def save_to_file(filename, data):
    """
    Save data to a file.

    Args:
        filename (str): Name of the file.
        data (bytes): Data to save.

    Returns:
        bool: True if saved successfully, False otherwise.
    """
    try:
        with open(filename, "wb") as f:
            f.write(data)
        print(f"Data saved to {filename}")
        return True
    except Exception as e:
        print(f"Failed to save data: {e}")
        return False


def load_from_file(filename):
    """
    Load data from a file.

    Args:
        filename (str): Name of the file.

    Returns:
        bytes: Loaded data.
    """
    try:
        with open(filename, "rb") as f:
            return f.read()
    except Exception as e:
        print(f"Failed to load file {filename}: {e}")
        return None


def main_menu():
    """
    Display the main menu and handle user input.
    """
    while True:
        print("\nSecure CLI Tool Menu:")
        print("1. Generate a new encryption key")
        print("2. Encrypt a message")
        print("3. Decrypt a message")
        print("4. Exit")
        choice = input("\nEnter your choice: ").strip()

        if choice == "1":
            key = generate_new_key()
            save_option = input("Would you like to save the key to a file? (y/n): ").strip().lower()
            if save_option == 'y':
                filename = input("Enter the filename to save the key (e.g., 'key.enc'): ").strip()
                save_to_file(filename, key)

        elif choice == "2":
            key = input("Enter your encryption key: ").strip().encode()
            message = input("Enter the message to encrypt: ")
            encrypted_message = encrypt_message(key, message)
            print("\nEncrypted Message:")
            print(encrypted_message.decode())
            save_option = input("Would you like to save the encrypted message to a file? (y/n): ").strip().lower()
            if save_option == 'y':
                filename = input("Enter the filename to save the encrypted message (e.g., 'message.enc'): ").strip()
                save_to_file(filename, encrypted_message)

        elif choice == "3":
            key = input("Enter your decryption key: ").strip().encode()
            load_option = input("Do you want to load the encrypted message from a file? (y/n): ").strip().lower()
            if load_option == 'y':
                filename = input("Enter the filename containing the encrypted message: ").strip()
                encrypted_message = load_from_file(filename)
            else:
                encrypted_message = input("Enter the encrypted message: ").strip().encode()

            decrypted_message = decrypt_message(key, encrypted_message)
            if decrypted_message:
                print("\nDecrypted Message:")
                print(decrypted_message)

        elif choice == "4":
            print("Exiting the Secure CLI Tool. Goodbye!")
            break

        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    """
    Entry point of the script. Executes the main menu loop.
    """
    print("Welcome to the Secure CLI Tool!")
    main_menu()
