import logging
import utils.file_ops as file_ops
from pathlib import Path
import setup.logging_setup as logging_setup # Function to initialise logger
from setup import config_setup  # Interfaces with config.ini functionalities

# Dynamically obtain the logger name from the script name (without extension).
config = config_setup.get_prod_config()
script_name: str = Path(__file__).stem


# Build the absolute path for the log file
logs_dir: Path = logging_setup.configure_logs_directory()
logfile_path: Path = Path(logs_dir, f"{script_name}.log")


# Get the logger instance
logger = logging_setup.get_logger(
            logger_name=script_name,
            logfile_name=logfile_path,
            console_level=logging.INFO,
            file_level=logging.DEBUG
        )

# Placeholder Python script
def main(file_path: str):
    try:
        logging.info(f"process {script_name} started")
        logging.info(f"processing file {file_path}")
        file_ops.move_file(file_path, "../processed")
        logging.info(f"process {script_name} completed")
        return True
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
        raise

if __name__ == "__main__":
    print("This is a placeholder script.")
    print("It should be called from within the pipeline.")
    print("and needs a path to a file to be passed.")
    file_to_be_processed = input("Enter the file path to process: ")
    main(file_to_be_processed)