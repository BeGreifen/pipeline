import logging
import os
import shutil
from pathlib import Path
from datetime import datetime


import setup.logging_setup as logging_setup # Function to initialise logger
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

def generate_timestamp() -> str:
    """
    Return the current date and time in a clear format.
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


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
        shutil.move(str(file_path), str(destination_path))
        logger.info(f"Moved file: {file_path} to {destination_path}")
        return destination_path
    except Exception as e:
        logger.error(f"Error moving file {file_path} to {destination_folder}: {e}")
        raise


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
        shutil.copy(str(file_path), str(destination_path))
        logger.info(f"Copied file: {file_path} to {destination_path}")
        return destination_path
    except Exception as e:
        logger.error(f"Error copying file {file_path} to {destination_folder}: {e}")
        raise


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
        new_path = file_path.with_name(new_name)
        file_path.rename(new_path)
        logger.info(f"Renamed file: {file_path} to {new_path}")
        return new_path
    except Exception as e:
        logger.error(f"Error renaming file {file_path} to {new_name}: {e}")
        raise
