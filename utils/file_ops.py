import logging
import os
import time
import shutil
from pathlib import Path
from datetime import datetime
from utils.cache_utils import cache_function

import setup.logging_setup as logging_setup  # Function to initialise logger
from setup import config_setup  # Interfaces with config.ini functionalities

# Dynamically obtain the logger name from the script name (without extension).
config = config_setup.get_prod_config()
script_name: str = Path(__file__).stem

# Build the absolute path for the log file
logs_dir: Path = logging_setup.configure_logs_directory()
logfile_path = os.path.join(logs_dir, f"{script_name}.log")

# Get the logger instance
logger = logging_setup.get_logger(
    logger_name=script_name,
    logfile_name=logfile_path,
    console_level=logging.INFO,
    file_level=logging.DEBUG
)


@cache_function(maxsize=256)
def check_file_is_ready(file_path: str,
                        checks: int = 3,
                        interval: float = 2.0,
                        timeout: float = 30.0) -> bool:
    """
        Checks if a file is ready by verifying that its size has not changed for a specified number of
        consecutive checks within a given timeout period.

        The function uses a caching mechanism to optimize frequent calls with identical arguments. If the
        file's size remains stable for a specified number of consecutive checks, it is considered ready. The
        function ensures the check respects the provided time interval and stops if the operation exceeds the
        specified timeout.

        Parameters:
        file_path (str): Path to the file to check.
        checks (int): Number of consecutive checks to confirm the file's stability. Default is 3.
        interval (float): Time interval in seconds between consecutive checks. Default is 2.0.
        timeout (float): Maximum duration in seconds to wait for the file to become stable. Default is 30.0.

        Returns:
        bool: True if the file is ready; otherwise, False.

        Raises:
        None
    """
    start_time = time.time()
    file_path_obj = Path(file_path)
    if not file_path_obj.is_file():
        logger.warning(f"File '{file_path}' does not exist or is not a regular file.")
        return False

    stable_count = 0
    last_size = os.path.getsize(file_path)
    while True:
        time.sleep(interval)
        current_size = os.path.getsize(file_path)
        if current_size == last_size:
            stable_count += 1
        else:
            stable_count = 0  # Reset if the file size changes
        last_size = current_size

        if stable_count >= checks:
            logger.debug(f"File '{file_path}' is ready (stable for {checks} consecutive checks).")
            return True
        if (time.time() - start_time) > timeout:
            logger.error(f"Timeout: File '{file_path}' did not become stable within {timeout} seconds.")
            return False


@cache_function(maxsize=256)
def wait_until_file_ready(file_path: str,
                          check_interval: float = 1.0,
                          max_wait: float = 300.0,
                          readiness_checks: int = 3,
                          readiness_interval: float = 2.0,
                          readiness_timeout: float = 30.0) -> bool:
    """
    Wait until a file is ready for processing. The file is considered ready when its
    size remains stable over a set number of checks. If the file is not ready within
    the specified max_wait time, the function returns False.

    Args:
        file_path (str): Path to the file to be checked.
        check_interval (float): Delay (in seconds) between successive readiness attempts.
        max_wait (float): Maximum total time (in seconds) to wait for the file to be ready.
        readiness_checks (int): Number of consecutive stable size checks required per attempt.
        readiness_interval (float): Delay (in seconds) between size checks in a single attempt.
        readiness_timeout (float): Timeout (in seconds) for each individual attempt.

    Returns:
        bool: True if the file becomes ready within the allowed time, otherwise False.
    """
    start_time = time.time()
    while True:
        if check_file_is_ready(
                file_path=file_path,
                checks=readiness_checks,
                interval=readiness_interval,
                timeout=readiness_timeout
        ):
            return True

        elapsed_time = time.time() - start_time
        if elapsed_time >= max_wait:
            logger.error(f"Max wait time of {max_wait} seconds exceeded for file '{file_path}'.")
            return False

        logger.info(f"File '{file_path}' is not ready yet. Retrying in {check_interval} second(s)...")
        time.sleep(check_interval)


@cache_function(maxsize=256)
def generate_timestamp() -> str:
    """
    Return the current date and time in a clear format.
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


@cache_function(maxsize=256)
def create_directory(directory_path: str) -> Path:
    """
    Ensure the directory exists; if not, create it.

    Args:
        directory_path (str): Path to the directory.

    Returns:
        Path: The created or existing directory as a Path object.
    """
    path = Path(directory_path)
    path.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Ensured directory exists: {directory_path}")
    return path


@cache_function(maxsize=256)
def move_file(file_path: str, destination_folder: str) -> Path:
    """
    Move a file to a specified folder.

    Args:
        file_path (str): Path to the file to be moved.
        destination_folder (str): Directory where the file should be moved.

    Returns:
        Path: The new path of the moved file.
    """
    try:
        file_path = Path(file_path)
        destination_folder = Path(destination_folder)
        create_directory(str(destination_folder))  # Ensure destination exists
        destination_path = destination_folder / file_path.name
        wait_until_file_ready(str(file_path))
        shutil.move(str(file_path), str(destination_path))
        logger.debug(f"Moved file: {file_path} to {destination_path}")
        return destination_path
    except Exception as e:
        logger.error(f"Error moving file {file_path} to {destination_folder}: {e}")
        raise


@cache_function(maxsize=256)
def copy_file(file_path: str, destination_folder: str) -> Path:
    """
    Copy a file to a specified folder.

    Args:
        file_path (str): Path to the file to be copied.
        destination_folder (str): Directory where the file should be copied.

    Returns:
        Path: The new path of the copied file.
    """
    try:
        file_path = Path(file_path)
        destination_folder = Path(destination_folder)
        create_directory(str(destination_folder))  # Ensure destination exists
        destination_path = destination_folder / file_path.name
        wait_until_file_ready(str(file_path))
        shutil.copy(str(file_path), str(destination_path))
        logger.debug(f"Copied file: {file_path} to {destination_path}")
        return destination_path
    except Exception as e:
        logger.error(f"Error copying file {file_path} to {destination_folder}: {e}")
        raise


@cache_function(maxsize=256)
def rename_file(file_path: str, new_name: str) -> Path:
    """
    Rename a file to a new name.

    Args:
        file_path (str): Path to the file to be renamed.
        new_name (str): New name (with extension) for the file.

    Returns:
        Path: The renamed file path.
    """
    try:
        file_path = Path(file_path)
        wait_until_file_ready(str(file_path))
        new_path = file_path.with_name(new_name)
        file_path.rename(new_path)
        logger.debug(f"Renamed file: {file_path} to {new_path}")
        return new_path
    except Exception as e:
        logger.error(f"Error renaming file {file_path} to {new_name}: {e}")
        raise
