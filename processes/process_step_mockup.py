import logging
import functools

from pathlib import Path
import setup.logging_setup as logging_setup # Function to initialise logger
from setup import config_setup  # Interfaces with config.ini functionalities
import random
import time

from utils.cache_utils import cache_function

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
def log_exceptions_with_args(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Log the error with function name and parameter details.
            logger.error(
                "Exception in function '%s' with args: %s, kwargs: %s",
                func.__name__,
                args,
                kwargs,
                exc_info=True  # This logs the traceback as well.
            )
            raise  # Re-raise the exception after logging.
    return wrapper


# Placeholder Python script
@cache_function(maxsize=256)
@log_exceptions_with_args
def main(file_path: str):
    # code to process file here:
    # ...
    logger.debug(f"processing file {file_path} ")
    temp_var: int = random.randint(0, 5)
    logger.debug(f"Waiting for {temp_var} second(s)")
    # Append a line to the file in file_path indicating the process step and wait time.
    with open(str(file_path), "a", encoding="utf-8") as temp_file:
        temp_file.write(f"Process step: waited for {temp_var} second(s)\n")
    time.sleep(float(temp_var))
    # ...
    # finally move processed file to the process_dir of the stage
    return str(temp_file.name)


if __name__ == "__main__":
    print("This is a placeholder script.")
    print("It should be called from within the pipeline.")
    print("and needs a path to a file to be passed.")
    file_to_be_processed = input("Enter the file path to process: ")
    main(file_to_be_processed)