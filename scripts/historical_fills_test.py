import os
import sys

class HistoricalFillsTest:
    """
    Class to test historical fills by manually running the scripts.
    """

    def __init__(self):
        """
        Initialize the class.
        """
        self.options = {
            "1": ("Run Hourly Bulk Fill", self.run_hourly_fill),
            "2": ("Run Daily Bulk Fill", self.run_daily_fill),
            "3": ("Exit", self.exit_program),
        }

    def menu(self):
        """
        Display a menu for selecting which historical fill script to run.
        """
        while True:
            self.display_menu()
            choice = input("Select an option: ").strip()
            action = self.options.get(choice, (None, self.invalid_choice))[1]
            action()

    def display_menu(self):
        """
        Print the menu options to the console.
        """
        print("\nHistorical Fills Test")
        print("-" * 30)
        for key, (description, _) in self.options.items():
            print(f"{key}. {description}")
        print("-" * 30)

    def run_hourly_fill(self):
        """
        Execute the hourly historical fill script.
        """
        print("\nRunning Hourly Bulk Fill...")
        os.system("python openweather_historical_fill_hourly.py")

    def run_daily_fill(self):
        """
        Execute the daily historical fill script.
        """
        print("\nRunning Daily Bulk Fill...")
        os.system("python openweather_historical_fill_daily.py")

    def exit_program(self):
        """
        Exit the program.
        """
        print("\nExiting Historical Fills Test...")
        sys.exit(0)

    def invalid_choice(self):
        """
        Handle invalid menu selections.
        """
        print("Invalid choice. Please select a valid option.")
        input("Press Enter to continue...")

if __name__ == "__main__":
    """
    Entry point for standalone execution.
    """
    tester = HistoricalFillsTest()
    tester.menu()
