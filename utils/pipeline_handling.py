import os
import configparser
from pathlib import Path
from typing import Optional
from utils.file_ops import move_file, copy_file

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read("config.ini")

# Configuration parameters
BASE_DIR: str = config["PIPELINE"].get("base_dir", "")
PIPELINE_STORAGE_DIR: str = config["PIPELINE"].get("pipeline_storage_dir", "")
FUNCTION_PREFIX: str = config["PIPELINE"].get("prefix", "pipeline_step_")
SUCCESS_FOLDER: str = config["PIPELINE"].get("success_folder", "")
ERROR_FOLDER: str = config["PIPELINE"].get("error_folder", "")


def get_next_folder(current_folder: str) -> Optional[str]:
    """
    Get the next folder alphabetically in the pipeline.

    Args:
        current_folder (str): The path of the current pipeline folder.

    Returns:
        Optional[str]: The path to the next folder in the pipeline, or None
                       if the current folder is the last one.
    """
    # Get all sibling folders in alphabetical order, excluding "working"
    sibling_folders = sorted(
        folder
        for folder in os.listdir(BASE_DIR)
        if os.path.isdir(os.path.join(BASE_DIR, folder)) and folder != "working"
    )

    # Get the current folder's index
    current_folder_name = Path(current_folder).name
    current_index = sibling_folders.index(current_folder_name)

    # Return the next folder if it exists, else None
    if current_index + 1 < len(sibling_folders):
        return str(Path(BASE_DIR) / sibling_folders[current_index + 1])
    return None


def get_processor_function(folder_name: str):
    """
    Dynamically imports and retrieves the processor function for the given folder.

    Args:
        folder_name (str): The name of the folder (e.g., "10_raw_pdf").

    Returns:
        callable: The processor function.

    Raises:
        ImportError: If the processor function cannot be found or imported.
    """
    processor_function_name: str = f"{FUNCTION_PREFIX}{folder_name}"
    try:
        module_ref = __import__("processors", fromlist=[processor_function_name])
        return getattr(module_ref, processor_function_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Processor function {processor_function_name} not found: {e}")


def create_working_dir(folder_path: str) -> str:
    """
    Create a working directory inside the given folder if it doesn't already exist.

    Args:
        folder_path (str): The parent folder where the working directory will be created.

    Returns:
        str: The path to the working directory.
    """
    working_dir: str = str(Path(folder_path) / "working")
    Path(working_dir).mkdir(parents=True, exist_ok=True)
    return working_dir


def reflect_to_pipeline_storage(current_folder: str, filename: str) -> None:
    """
    Save a copy of the file to the corresponding database folder,
    maintaining the pipeline's folder structure.

    Args:
        current_folder (str): The current pipeline folder.
        filename (str): The name of the file to reflect into the database.

    """
    db_folder: str = str(Path(PIPELINE_STORAGE_DIR) / Path(current_folder).name)
    Path(db_folder).mkdir(parents=True, exist_ok=True)
    copy_file(str(Path(current_folder) / filename), str(Path(db_folder) / filename))


def process_file(file_path: str) -> None:
    """
    Processes a file through the pipeline with error handling and database mirroring.

    Args:
        file_path (str): The complete path of the file to be processed.

    Behavior:
    - Moves the file to a "working" directory for the current step.
    - Attempts to process the file using the step's dynamic processor function.
    - On success, moves the file to the next step's folder and mirrors it in the database.
    - On failure, renames the file indicating the error and mirrors the error state.
    """
    if not Path(file_path).exists():
        raise FileNotFoundError(f"The file {file_path} does not exist!")

    # Determine current folder and working directory
    current_folder: str = str(Path(file_path).parent)
    current_folder_name: str = Path(current_folder).name
    working_dir: str = create_working_dir(current_folder)

    # Define paths for the working file
    working_file_path: str = str(Path(working_dir) / Path(file_path).name)

    try:
        # Move the file to the working directory
        move_file(file_path, working_file_path)

        # Get and execute the processor function for the current folder
        processor_func = get_processor_function(current_folder_name)
        result: bool = processor_func(working_file_path)

        if result:
            # Reflect success in database and move to the next step
            reflect_to_pipeline_storage(current_folder, Path(file_path).name)
            next_folder: Optional[str] = get_next_folder(current_folder)

            if next_folder:
                # Move to the next step
                move_file(working_file_path, str(Path(next_folder) / Path(file_path).name))
            else:
                # If no next step exists, move to the success folder
                move_file(working_file_path, str(Path(SUCCESS_FOLDER) / Path(file_path).name))
        else:
            # Handle processing failure
            handle_processing_error(current_folder, file_path, working_file_path)

    except Exception as e:
        # Handle unexpected errors by moving the original file to the error folder
        print(f"Unexpected error while processing file {file_path}: {str(e)}")
        move_file(file_path, str(Path(ERROR_FOLDER) / Path(file_path).name))


def handle_processing_error(current_folder: str, original_file: str, working_file: str) -> None:
    """
    Handle errors encountered during processing by tracking both the
    original file and the working file.

    Args:
        current_folder (str): The current pipeline step folder.
        original_file (str): The original file being processed.
        working_file (str): The working file that failed processing.

    Raises:
        None
    """
    # Append `_work_error` to the working file
    work_error_file = str(
        Path(working_file).with_name(f"{Path(working_file).stem}_work_error{Path(working_file).suffix}"))
    os.rename(working_file, work_error_file)

    # Append `_causing_error` to the original file and reflect in the database
    causing_error_file = str(
        Path(original_file).with_name(f"{Path(original_file).stem}_causing_error{Path(original_file).suffix}"))
    reflect_to_pipeline_storage(current_folder, f"{Path(original_file).stem}_causing_error{Path(original_file).suffix}")

    print(f"Processing error detected: {causing_error_file}, {work_error_file}")