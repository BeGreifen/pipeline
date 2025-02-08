import logging
import functools
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
@log_exceptions_with_args
def main(file_path: str):
    print(f"processing file {file_path}")
    print(f"the logs are stored: {logfile_path}")
    logger.info(f"logger started")

    file_path = Path(file_path).resolve()
    logger.debug(f"file path: {file_path}")

    processed_dir = file_path.parent.parent / "processed/"
    logger.debug(f"file path: {file_path}")
    try:
        logger.info(f"process {script_name} started")
        logger.info(f"processing file {file_path} and moving to {processed_dir}")
        file_ops.move_file(str(Path(file_path)), str(processed_dir))
        logger.info(f"process {script_name} completed")
        return True
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        raise

if __name__ == "__main__":
    print("This is a placeholder script.")
    print("It should be called from within the pipeline.")
    print("and needs a path to a file to be passed.")
    file_to_be_processed = input("Enter the file path to process: ")
    main(file_to_be_processed)