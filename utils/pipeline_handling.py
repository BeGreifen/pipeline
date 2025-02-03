import os
import configparser
from pathlib import Path
from typing import Optional
from utils.file_ops import move_file, copy_file, rename_file
import logging
import setup.logging_setup as logging_setup

# Get the logger instance
logger = logging_setup.get_logger(
    logger_name="pipeline_handling",
    logfile_name="pipeline_handling.log",
    console_level=logging.INFO,
    file_level=logging.DEBUG
)

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read("config.ini")

# Configuration parameters
BASE_DIR: str = config["PIPELINE"].get("base_dir", "")
PIPELINE_STORAGE_DIR: str = config["PIPELINE"].get("pipeline_storage_dir", "")
FUNCTION_PREFIX: str = config["PIPELINE"].get("prefix", "pipeline_step_")
SUCCESS_DIR: str = config["PIPELINE"].get("success_dir", "")
ERROR_DIR: str = config["PIPELINE"].get("error_dir", "")


def get_next_dir(current_dir: str) -> Optional[str]:
    """
    Get the next folder alphabetically in the pipeline.

    Args:
        current_dir (str): The path of the current pipeline folder.

    Returns:
        Optional[str]: The path to the next folder in the pipeline, or None
                       if the current folder is the last one.
    """
    # Get all sibling folders in alphabetical order, excluding "working"
    sibling_dir = sorted(
        folder
        for folder in os.listdir(BASE_DIR)
        if os.path.isdir(os.path.join(BASE_DIR, folder)) and folder != "working"
    )

    # Get the current folder's index
    current_dir_name = Path(current_dir).name
    current_index = sibling_dir.index(current_dir_name)

    # Return the next folder if it exists, else None
    if current_index + 1 < len(sibling_dir):
        return str(Path(BASE_DIR) / sibling_dir[current_index + 1])
    return None


def get_processor_function(dir_name: str):
    """
    Dynamically imports and retrieves the processor function for the given folder.

    Args:
        dir_name (str): The name of the folder (e.g., "10_raw_pdf").

    Returns:
        callable: The processor function.

    Raises:
        ImportError: If the processor function cannot be found or imported.
    """
    processor_function_name: str = f"{FUNCTION_PREFIX}{dir_name}"
    try:
        module_ref = __import__("processors", fromlist=[processor_function_name])
        return getattr(module_ref, processor_function_name)
    except (ImportError, AttributeError) as e:
        logger.error(f"Processor function {processor_function_name} not found: {e}")
        raise ImportError(f"Processor function {processor_function_name} not found: {e}")


def create_working_dir(dir_path: str) -> str:
    """
    Create a working directory inside the given folder if it doesn't already exist.

    Args:
        dir_path (str): The parent folder where the working directory will be created.

    Returns:
        str: The path to the working directory.
    """
    working_dir: str = str(Path(dir_path) / "working")
    Path(working_dir).mkdir(parents=True, exist_ok=True)
    return working_dir


def reflect_to_pipeline_storage(current_dir: str, file_path: str, result: bool) -> None:
    """
    Reflects the file into the appropriate location inside the pipeline storage directory
    after processing has been completed.

    Args:
        current_dir (str): The current pipeline step folder.
        file_path (str): The full path of the file being processed.
        result (bool): Indicates if the processing was successful (True) or failed (False).
    """
    try:
        # Create and ensure pipeline storage directory for the current folder
        storage_dir = Path(PIPELINE_STORAGE_DIR) / Path(current_dir).name
        storage_dir.mkdir(parents=True, exist_ok=True)

        file_name = Path(file_path).name

        if result:
            # If processing is successful, copy the file as is
            copy_file(file_path, str(storage_dir))
            logger.info(f"File successfully reflected to storage: {file_name}")
        else:
            # If processing fails, append "_causing_error" to the file name
            error_file_name = f"{Path(file_path).stem}_causing_error{Path(file_path).suffix}"
            copy_file(file_path, str(storage_dir))
            rename_file(str(storage_dir / file_name), error_file_name)
            logger.info(f"Error file reflected to storage: {error_file_name}")
    except Exception as e:
        # Handle any exceptions during reflection
        logger.error(f"Error reflecting file to pipeline storage: {file_path}. Exception: {e}")
        raise

def process_file(file_path: str) -> None:
    """
    Processes a file through the pipeline with error handling and database mirroring.

    Args:
        file_path (str): The complete path of the file to be processed.

    Behavior:
    - Moves the file to a "working" directory for the current step.
    - Attempts to process the file using the step's dynamic processor function.
    - On success, moves the file to the next step's folder and reflects it in pipeline storage.
    - On failure, renames the file indicating the error and reflects failure in pipeline storage.
    """
    if not Path(file_path).exists():
        logger.error(f"File does not exist: {file_path}")
        raise FileNotFoundError(f"The file {file_path} does not exist!")

    # Determine current folder and working directory
    current_dir: str = str(Path(file_path).parent)
    current_dir_name: str = Path(current_dir).name
    working_dir: str = create_working_dir(current_dir)

    # Define paths for the working file
    working_file_path: str = str(Path(working_dir) / Path(file_path).name)

    try:
        # Move the file to the working directory
        move_file(file_path, working_file_path)

        # Get and execute the processor function for the current folder
        processor_func = get_processor_function(current_dir_name)
        result: bool = processor_func(working_file_path)

        # Reflect the file in pipeline storage
        reflect_to_pipeline_storage(current_dir, working_file_path, result)

        if result:
            # Move to next folder or success folder
            next_dir: Optional[str] = get_next_dir(current_dir)
            if next_dir:
                move_file(working_file_path, str(Path(next_dir) / Path(file_path).name))
            else:
                move_file(working_file_path, str(Path(SUCCESS_DIR) / Path(file_path).name))
        else:
            # Handle processing failure
            handle_processing_error(current_dir, file_path, working_file_path)

    except Exception as e:
        # On unexpected errors, ensure the file is moved to the error folder
        logger.error(f"Unexpected error while processing file {file_path}: {e}")
        move_file(file_path, str(Path(ERROR_DIR) / Path(file_path).name))


def handle_processing_error(current_dir: str, original_file: str, working_file: str) -> None:
    """
    Handles processing errors by renaming files to indicate an error state and
    updating the pipeline storage to reflect these changes. This facilitates
    tracking and debugging of files that encountered processing issues. The
    modified files are properly renamed, and their updated names are logged for
    further inspection.

    :param current_dir: The directory path where the pipeline operates.
    :type current_dir: str
    :param original_file: The path to the original file that caused the error.
    :type original_file: str
    :param working_file: The path to the working file being processed when the error occurred.
    :type working_file: str
    :return: This function does not return any value.
    :rtype: None
    """
    # Append `_work_error` to the working file
    work_error_file = str(
        Path(working_file).with_name(f"{Path(working_file).stem}_work_error{Path(working_file).suffix}"))
    os.rename(working_file, work_error_file)

    # Append `_causing_error` to the original file and reflect in the database
    causing_error_file = str(
        Path(original_file).with_name(f"{Path(original_file).stem}_causing_error{Path(original_file).suffix}"))
    reflect_to_pipeline_storage(current_dir, f"{Path(original_file).stem}_causing_error{Path(original_file).suffix}")

    logger.info(f"Processing error detected: {causing_error_file}, {work_error_file}")

def purge_pipeline_storage():
    """
    Deletes all files and folders inside the pipeline storage directory
    while keeping the main directory intact.

    Raises:
        FileNotFoundError: If the pipeline storage directory doesn't exist.
    """
    if not PIPELINE_STORAGE_DIR:
        raise ValueError("PIPELINE_STORAGE_DIR is not configured.")

    pipeline_storage_path = Path(PIPELINE_STORAGE_DIR)

    if not pipeline_storage_path.exists():
        raise FileNotFoundError(f"Pipeline storage directory '{PIPELINE_STORAGE_DIR}' does not exist.")

    try:
        # Loop through and delete all files and folders in the pipeline storage directory
        for item in pipeline_storage_path.iterdir():
            if item.is_file():
                item.unlink()  # Remove file
            elif item.is_dir():
                # Recursively delete directory
                for sub_item in item.iterdir():
                    if sub_item.is_file():
                        sub_item.unlink()
                    elif sub_item.is_dir():
                        sub_item.rmdir()
                item.rmdir()  # Remove the directory itself

        logger.info(f"Pipeline storage directory '{PIPELINE_STORAGE_DIR}' has been purged.")
    except Exception as e:
        logger.error(f"Error while purging pipeline storage: {e}")
        raise
