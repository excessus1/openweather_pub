"""
Config Menu Script

This script provides a user interface for managing configurations, allowing
users to view, create, update, reset, and select configuration profiles.
"""

from pathlib import Path
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from src.config.config_manager import ConfigManager


class ConfigMenu:
    """
    The ConfigMenu class provides methods for interacting with configuration profiles.
    """

    def __init__(self, config_manager):
        """
        Initialize the ConfigMenu class.

        Args:
            config_manager (ConfigManager): Instance of ConfigManager for profile operations.
        """
        self.config_manager = config_manager

    def clear_screen(self):
        """Clear the console screen."""
        os.system('cls' if os.name == 'nt' else 'clear')

    def main_menu(self):
        """
        Display and handle the main menu interface for configuration operations.
        """
        while True:
            self.clear_screen()
            print("Configuration Menu")
            print("-" * 20)
            print("1. View Profiles")
            print("2. Create Custom Profile")
            print("3. Update Custom Profile")
            print("4. Reset Default Profile")
            print("5. Set Selected Config")
            print("6. Exit")
            choice = input("\nSelect an option: ").strip()

            if choice == "1":
                self.view_profiles()
            elif choice == "2":
                self.create_custom_profile()
            elif choice == "3":
                self.update_custom_profile()
            elif choice == "4":
                self.reset_default_profile()
            elif choice == "5":
                self.set_selected_config()
            elif choice == "6":
                break
            else:
                print("Invalid choice. Please try again.")
                input("Press Enter to continue...")

    def set_selected_config(self):
        """
        Set the selected configuration profile for the application.
        """
        self.clear_screen()
        print("Set Selected Config")
        print("-" * 20)

        profiles = ["default_profile"] + list(self.config_manager.custom_profiles.keys())
        for i, profile in enumerate(profiles, 1):
            print(f"{i}. {profile}")

        try:
            choice = int(input("\nSelect a profile to set as selected config (or 0 to return): ").strip())
            if choice == 0:
                return
            profile_name = profiles[choice - 1]
            self.config_manager.write_selected_config(profile_name)
        except (ValueError, IndexError):
            print("Invalid selection.")
            input("Press Enter to continue...")

    def view_profiles(self):
        """
        View all available profiles and display details of a selected profile.
        """
        self.clear_screen()
        print("Existing Profiles")
        print("-" * 20)

        # Display profiles
        profiles = ["default_profile"] + list(self.config_manager.custom_profiles.keys())
        for i, profile in enumerate(profiles, 1):
            print(f"{i}. {profile}")

        # Select a profile to view
        try:
            choice = int(input("\nSelect a profile to view (or 0 to return): ").strip())
            if choice == 0:
                return
            profile_name = profiles[choice - 1]
            profile = (self.config_manager.default_profile if profile_name == "default_profile"
                       else self.config_manager.custom_profiles[profile_name])
            self.display_profile(profile_name, profile)
        except (ValueError, IndexError):
            print("Invalid selection.")
            input("Press Enter to continue...")

    def display_profile(self, profile_name, profile):
        """
        Display detailed information about a specific profile.

        Args:
            profile_name (str): Name of the profile to display.
            profile (dict): Profile data to display.
        """
        self.clear_screen()
        print(f"Profile: {profile_name}")
        print("-" * 20)

        def print_nested_dict(d, indent=0):
            for key, value in d.items():
                if isinstance(value, dict):
                    print("  " * indent + f"{key}:")
                    print_nested_dict(value, indent + 1)
                else:
                    print("  " * indent + f"{key}: {value}")

        print_nested_dict(profile)
        input("\nPress Enter to return to the menu...")

    def create_custom_profile(self):
        """
        Guide the user through the process of creating a custom configuration profile.
        """
        self.clear_screen()
        print("Create a Custom Profile")
        print("-" * 20)

        while True:
            profile_name = input("Enter a unique profile name: ").strip()
            if not profile_name:
                print("Profile name cannot be empty.")
                continue
            if profile_name in self.config_manager.custom_profiles:
                print("Profile name already exists. Please choose another.")
                continue
            break

        # Create and populate the profile
        new_profile = self.config_manager.generate_profile()
        self.populate_profile(new_profile, is_default=False)
        self.config_manager.custom_profiles[profile_name] = new_profile
        self.config_manager.save_config()
        print(f"Custom profile '{profile_name}' created successfully.")
        input("Press Enter to return to the menu...")

    def update_custom_profile(self):
        """
        Allow the user to update an existing custom profile.
        """
        self.clear_screen()
        print("Update Custom Profile")
        print("-" * 20)

        # List existing profiles
        profiles = list(self.config_manager.custom_profiles.keys())
        if not profiles:
            print("No custom profiles available.")
            input("Press Enter to return to the menu...")
            return

        for i, profile_name in enumerate(profiles, 1):
            print(f"{i}. {profile_name}")

        # Select a profile to update
        try:
            choice = int(input("\nSelect a profile to update (or 0 to return): ").strip())
            if choice == 0:
                return
            selected_profile = profiles[choice - 1]
            profile = self.config_manager.custom_profiles[selected_profile]
            self.populate_profile(profile, is_default=False)
            self.config_manager.custom_profiles[selected_profile] = profile
            self.config_manager.save_config()
            print(f"Custom profile '{selected_profile}' updated successfully.")
        except (ValueError, IndexError):
            print("Invalid selection.")
        input("Press Enter to return to the menu...")

    def reset_default_profile(self):
        """
        Reset the default profile to its initial state.
        """
        self.config_manager.reset_config()
        print("Default profile has been reset to defaults.")
        input("Press Enter to return to the menu...")

    def populate_profile(self, profile, is_default):
        """
        Populate or update a profile with user-provided values.

        Args:
            profile (dict): Profile data to be updated.
            is_default (bool): Whether this is the default profile.
        """
        for category, settings in profile.items():
            print(f"\nConfiguring {category}")
            for setting, value in list(settings.items()):  # Use list to safely modify dictionary
                if setting.lower() == "limit_per_day" and not is_default:
                    del profile[category][setting]  # Ensure limit_per_day is removed
                    continue
                elif setting.lower().endswith("start"):
                    print(f"1. Set to 'recent'")
                    print(f"2. Enter a custom datetime (format: YYYY-MM-DD HH:MM:SS)")
                    print(f"Current value: {value}")
                    choice = input(f"{setting}: ").strip()
                    if choice == "1":
                        profile[category][setting] = "recent"
                    elif choice == "2":
                        custom_value = input(f"Enter datetime: ").strip()
                        profile[category][setting] = custom_value
                else:
                    new_value = input(f"{setting} (current: {value}): ").strip()
                    if new_value.isdigit():
                        profile[category][setting] = int(new_value)
                    elif new_value.lstrip('-').replace('.', '', 1).isdigit():
                        profile[category][setting] = float(new_value)
                    elif new_value:
                        profile[category][setting] = new_value
        # Save updated profile
        self.config_manager.save_config()

if __name__ == "__main__":
    """
    Run the script with a predefined configuration.
    """
    CONFIG_FILE = "../src/config/config.json"
    FORMAT_SCHEMA_FILE = "../src/config/profile_format_schema.json"
    DEFAULT_VALUES_FILE = "../src/config/profile_default_values.json"

    config_manager = ConfigManager(CONFIG_FILE, FORMAT_SCHEMA_FILE, DEFAULT_VALUES_FILE)
    menu = ConfigMenu(config_manager)
    menu.main_menu()
