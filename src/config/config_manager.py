import json
from pathlib import Path
from datetime import datetime


class ConfigManager:
    """
    A manager for handling configuration files, including validation, resetting,
    and maintaining custom and default profiles.

    Attributes:
        config_file (Path): The path to the configuration file.
        format_schema (dict): The schema used to validate profiles.
        default_values (dict): Default values for profile fields.
        default_profile (dict): Default profile data.
        custom_profiles (dict): User-defined profiles.
        selected_config (str): The name of the currently selected profile.
    """

    def __init__(self, config_file, format_schema_file, default_values_file):
        """
        Initialize the ConfigManager.

        Args:
            config_file (str): Path to the configuration file.
            format_schema_file (str): Path to the file containing the validation schema.
            default_values_file (str): Path to the file containing default values.

        Raises:
            FileNotFoundError: If any required file is not found.
            ValueError: If the schema or default values contain invalid JSON.
        """
        self.config_file = Path(config_file)
        self.format_schema = self.load_schema(format_schema_file)
        self.default_values = self.load_schema(default_values_file)
        self.default_profile = {}
        self.custom_profiles = {}
        self.selected_config = None  # Currently selected profile name
        self.load_config()

    def load_schema(self, schema_file):
        """
        Load a JSON schema from a file.

        Args:
            schema_file (str): Path to the schema file.

        Returns:
            dict: The parsed schema.

        Raises:
            FileNotFoundError: If the schema file does not exist.
            ValueError: If the schema file contains invalid JSON.
        """
        try:
            with open(schema_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file '{schema_file}' not found.")
        except json.JSONDecodeError:
            raise ValueError(f"Schema file '{schema_file}' contains invalid JSON.")

    def load_config(self):
        """
        Load the configuration file and validate its contents.

        If the file is missing or invalid, the configuration is reset.
        """
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    raw_config = json.load(f)

                # Debug: Print loaded configuration
                print("Loaded configuration:", json.dumps(raw_config, indent=2))

                self.selected_config = raw_config.get("selected_config")
                self.validate_and_parse_config(raw_config)
            except json.JSONDecodeError:
                print(f"Invalid JSON in configuration file '{self.config_file}'. Resetting configuration.")
                self.reset_config()
        else:
            print(f"Configuration file '{self.config_file}' not found. Creating new configuration.")
            self.reset_config()

    def reset_config(self):
        """
        Reset the configuration to default values.

        Clears custom profiles and sets the default profile as the active configuration.
        """
        self.default_profile = self.generate_profile()
        self.custom_profiles = {}
        self.selected_config = None
        self.save_config()

    def save_config(self):
        """
        Save the current configuration to the configuration file.
        """
        config = {
            "default_profile": self.default_profile,
            "custom_profiles": self.custom_profiles,
            "selected_config": self.selected_config,
        }
        with open(self.config_file, 'w') as f:
            json.dump(config, f, indent=2)
        print(f"Configuration saved to '{self.config_file}'.")

    def generate_profile(self):
        """
        Generate a new default profile using predefined values.

        Returns:
            dict: A dictionary containing the default profile data.
        """
        return {category: values.copy() for category, values in self.default_values.items()}

    def validate_and_parse_config(self, raw_config):
        """
        Validate and parse the loaded configuration.

        Args:
            raw_config (dict): The raw configuration data loaded from the file.

        This updates the default and custom profiles if validation succeeds.
        """
        self.default_profile = raw_config.get("default_profile", {})
        print("Validating default profile:", json.dumps(self.default_profile, indent=2))
        self.validate_profile(self.default_profile, is_default=True)

        self.custom_profiles = raw_config.get("custom_profiles", {})
        for profile_name, profile_data in self.custom_profiles.items():
            print(f"Validating custom profile '{profile_name}':", json.dumps(profile_data, indent=2))
            self.validate_profile(profile_data)

    def validate_profile(self, profile, is_default=False):
        """
        Validate a profile against the format schema.

        Args:
            profile (dict): The profile data to validate.
            is_default (bool): Whether the profile is the default profile.

        This ensures all required categories and fields are present, filling in missing defaults.
        """
        for category, fields in self.format_schema.items():
            # Ensure category exists in profile
            if category not in profile:
                if is_default:
                    print(f"Missing category '{category}' in default profile. Adding defaults.")
                    profile[category] = self.default_values.get(category, {})
                else:
                    print(f"Warning: Missing category '{category}' in custom profile.")
                continue

            for field, rules in fields.items():
                # Ensure field exists in category
                if field not in profile[category]:
                    if is_default:
                        print(f"Missing field '{field}' in default profile. Adding default value.")
                        profile[category][field] = self.default_values[category].get(field)
                    else:
                        print(f"Warning: Missing field '{field}' in custom profile '{category}'.")
                    continue

                # Validate and correct existing values without overwriting
                profile[category][field] = self.correct_value(field, profile[category][field], rules, category)

    def correct_value(self, key, value, rules, category):
        """
        Validate and correct profile field values.

        Args:
            key (str): Field name.
            value: Current value of the field.
            rules (dict): Validation rules for the field.
            category (str): The profile category the field belongs to.

        Returns:
            Corrected value.

        Raises:
            ValueError: If the value does not meet validation criteria.
        """
        value_type = rules["type"]
        if value_type == "int":
            # Convert string to integer if necessary
            if isinstance(value, str) and value.isdigit():
                value = int(value)
            elif not isinstance(value, int):
                raise ValueError(f"Invalid value for {key}: {value}. Must be an integer.")

            if key == "batch_limit":
                # Enforce constraint: batch_limit <= limit_per_day in the same category
                default_limit = self.default_profile.get(category, {}).get("limit_per_day", float("inf"))
                if value > default_limit:
                    raise ValueError(f"Invalid value for {key}: {value}. Must not exceed {default_limit}.")
            return value
        elif value_type == "float":
            return float(value)
        elif value_type == "datetime":
            self.validate_datetime(key, value, rules)
            return value
        return value

    def validate_datetime(self, key, value, rules):
        """
        Validate datetime fields against specified rules.

        Args:
            key (str): Field name.
            value (str): Current value of the field.
            rules (dict): Validation rules for the field.

        Raises:
            ValueError: If the value is invalid or does not match the required format.
        """
        if value == "" or value is None:
            raise ValueError(f"Invalid datetime value for {key}: '{value}'. Expected a non-empty string.")

        if value.lower() == "recent":
            return  # Allow "recent" for START fields
        try:
            format_str = rules["format"]
            format_str = format_str.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
            format_str = format_str.replace("HH", "%H").replace("MI", "%M").replace("SS", "%S")
            datetime.strptime(value, format_str)
        except ValueError:
            raise ValueError(f"Invalid datetime format for {key}: {value}. Expected format: {rules['format']}.")

    def write_selected_config(self, profile_name):
        """
        Update the selected configuration.

        Args:
            profile_name (str): Name of the profile to set as the selected configuration.

        Raises:
            KeyError: If the specified profile does not exist.
        """
        if profile_name not in self.custom_profiles and profile_name != "default_profile":
            print(f"Profile '{profile_name}' does not exist.")
            return
        self.selected_config = profile_name
        self.save_config()
        print(f"Selected config set to '{profile_name}'.")
