import logging
import functools
import utils.file_ops as file_ops
from pathlib import Path
import setup.logging_setup as logging_setup  # Function to initialise logger
from setup import config_setup  # Interfaces with config.ini functionalities

from utils.cache_utils import cache_function
from processes import process_step_mockup

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
    """
    Provides a decorator to log exceptions raised by the wrapped functions,
    including their arguments and traceback information. Ensures errors are
    documented for debugging while re-raising the exceptions for further handling.

    Parameters
    ----------
    func : callable
        The function to be wrapped by the decorator.

    Returns
    -------
    callable
        The wrapped function that includes logging functionality for exceptions.

    Raises
    ------
    Exception
        Re-raises any exception encountered while executing the wrapped function.
    """

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
def main(file_path: str) -> bool:
    """
    Process a given file and stores logs about the operation.

    The function performs operations such as resolving the file path, logging the
    process, calling another function to process the file, and finally moving the
    processed file to a specific directory for further stages.

    Arguments:
        file_path (str): The path of the file to be processed.

    Returns:
        bool: Returns True when the process is completed successfully.

    Raises:
        Any exceptions raised during the file processing or file moving operations
        are logged by the @log_exceptions_with_args decorator.
    """
    file_path = Path(file_path).resolve()
    processed_dir = file_path.parent.parent / "processed/"

    logger.info(f"process {script_name} for {file_path} started, output in {processed_dir}")

    # code to process file here:
    # ...
    file_processed_path = process_step_mockup.main(str(file_path))
    # ...
    # finally move processed file to the process_dir of the stage
    file_ops.move_file(str(Path(file_processed_path)), str(processed_dir))
    logger.info(f"process {script_name} completed and moving to {processed_dir}")
    return True


if __name__ == "__main__":
    print("This is a placeholder script.")
    print("It should be called from within the pipeline.")
    print("and needs a path to a file to be passed.")
    file_to_be_processed = input("Enter the file path to process: ")
    main(file_to_be_processed)
