# ==================== Standard library imports ====================
import os  # Provides operating system dependent functionality
import logging  # Offers logging operations
import importlib
import importlib.util # for the absolut path handling
import functools

from pathlib import Path  # Simplifies file path operations
from typing import Optional  # Supplies type hinting for optional parameters


# =================== Local module (project) imports ===================
from setup import config_setup  # Interfaces with config.ini functionalities
import setup.logging_setup as logging_setup  # Manages logging configuration
from utils.file_ops import move_file, copy_file, rename_file, create_directory, generate_timestamp  # Provides file operations

# Load configuration from config.ini
config = config_setup.get_prod_config()


# Dynamically obtain the logger name from the script name (without extension).
SCRIPT_NAME: str = Path(__file__).stem
PROJECT_ROOT: Path  = Path(__file__).parent.resolve()

# Build the absolute path for the log file
logs_dir: Path = logging_setup.configure_logs_directory()
logfile_path = os.path.join(logs_dir, f"{SCRIPT_NAME}.log")


# Get the logger instance
logger = logging_setup.get_logger(
    logger_name=SCRIPT_NAME,
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


# create global Abs Path Constants from Config
PROCESSES_DIR: Path = Path(config["PIPELINE"].get("processes_dir", "")).resolve()
logger.info(f"path to processes {PROCESSES_DIR}")
BASE_DIR: Path = Path(config["PIPELINE"].get("base_dir", "")).parent.resolve()
PIPELINE_DIR: Path = Path(config["PIPELINE"].get("pipeline_dir", "")).resolve()
PIPELINE_STORAGE_DIR: Path = Path(config["PIPELINE"].get("pipeline_storage_dir", "")).resolve()
SUCCESS_DIR: Path = Path(config["PIPELINE"].get("success_dir", "")).resolve()
ERROR_DIR: Path = Path(config["PIPELINE"].get("error_dir", "")).resolve()

# get function parameter from Config
PROCESS_FILE_PREFIX: str = config["PIPELINE"].get("process_file_prefix", "pipeline_step_")
PROCESS_FILE_FUNCTION_NAME: str = config["PIPELINE"].get("process_file_function_name", "process_this")


@log_exceptions_with_args
def get_next_dir(original_file_of_this_step_path: str) -> Optional[str]:
    """
    Get the next folder alphabetically in the pipeline.

    Args:
        original_file_of_this_step_path (str): A file in the path of the current pipeline folder.

    Returns:
        Optional[str]: The path to the next folder in the pipeline, or None
                       if the current folder is the last one.
    """
    # Get all sibling folders in alphabetical order
    sibling_dir = sorted(
        folder
        for folder in os.listdir(PIPELINE_DIR)
        if os.path.isdir(os.path.join(PIPELINE_DIR, folder))
    )
    logger.debug(f"sibling_dir {sibling_dir}")

    # Get the current folder's index
    current_dir_name = Path(original_file_of_this_step_path).name
    current_index = sibling_dir.index(original_file_of_this_step_path)

    # Return the next folder if it exists, else None
    if current_index + 1 < len(sibling_dir):
        return str(Path(BASE_DIR) / sibling_dir[current_index + 1])
    return None


@log_exceptions_with_args
def get_processor_function(step_name: str):
    """
    Dynamically imports the module for the given step name from the directory
    specified in the config, then returns the 'process_file' function.

    Args:
        step_name (str): Name of the step/module to import.

    Returns:
        function: The 'process_file' function in the imported module.

    Raises:
        ImportError: If the module or function can't be found.
    """
    try:
        # Convert relative config path to an absolute path
        logger.debug(f"path to processes {PROCESSES_DIR}")

        # getting process file name and create path
        process_file_name = f"{PROCESS_FILE_PREFIX}{step_name}.py"
        process_file_path = Path(PROCESSES_DIR) / Path(process_file_name)
        logger.debug(f"look for {process_file_name} in {PROCESSES_DIR}")
        logger.debug(f" -> {process_file_path}")


        try:
            spec = importlib.util.spec_from_file_location(PROCESS_FILE_FUNCTION_NAME,str(process_file_path))
            logger.debug(f"spec (=processor module) name '{spec.name}' origin {spec.origin} loaded")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module) #"execute" the module to get all attributes and functions -  this doesn't start any function of the module, just initiates it
            module_attribs = getattr(module, PROCESS_FILE_FUNCTION_NAME, None)
            logger.debug(f"module {module} with attribs {module_attribs} loaded")
        except (ImportError, ModuleNotFoundError) as err:
            # Handle or log the exception as needed
            logger.error(f"Failed to import {PROCESS_FILE_FUNCTION_NAME} from module '{process_file_name}': {err}")
            raise

        if module is None:
            raise AttributeError(f"Module '{process_file_name}' has no attribute '{PROCESS_FILE_FUNCTION_NAME}'")

        return module

    except (ImportError, AttributeError) as e:
        raise ImportError(f"Processor function {step_name} not found: {e}") from e


@log_exceptions_with_args
def create_working_dir(dir_path: str) -> str:  # not really used
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


@log_exceptions_with_args
def reflect_to_pipeline_storage(current_dir: str, file_path: str, result: bool = True) -> None:
    """
    Reflect (move and rename) a file into a pipeline storage subdirectory with
    a timestamp embedded in the new file name. Mirrors the directory structure
    by including the last portion of the file's original path in the new file name.

    Args:
        current_dir (str): The directory context in which this file was processed.
        file_path (str): The full path to the file to be reflected.
        result (bool): If False, reflection is skipped. If True, proceed.
    """
    if not result:
        logger.debug("Reflection skipped because 'result' is False.")
        return

    original_path: Path = Path(file_path)
    if not original_path.is_file():
        logger.warning(f"reflect_to_pipeline_storage file does not exist {file_path}")
        return

    # 1) Extract the “parent” directory name from the original file’s path
    #    (this helps capture dynamic provenance).
    parent_name: str = Path(current_dir).name  # e.g., "step3" from ".../step3/processed/"
    logger.info(f"parent_name {parent_name}")

    # 2) Prepare the pipeline storage subdirectory: we mirror the pipeline structure
    #    by creating a subdir for the current_dir.
    #    (Replace this path with your actual config lookup if needed.)
    pipeline_storage_base: Path = PIPELINE_STORAGE_DIR  # from config
    logger.info(f"pipeline_storage_base {pipeline_storage_base}")

    pipeline_storage_subdir: Path = pipeline_storage_base / parent_name
    logger.info(f"pipeline_storage_subdir {pipeline_storage_subdir}")

    create_directory(str(pipeline_storage_subdir))

    # 3) Include both the parent directory name and a timestamp in the new file name.
    file_status_derived_of_path: str = Path(file_path).parent.name
    step_name_derived_of_current_dir:str = Path(current_dir).parent.name
    status = file_status_derived_of_path if file_status_derived_of_path != parent_name else ""

        
    timestamp_str: str = generate_timestamp()
    new_file_name: str = f"{original_path.stem}_{status}_{timestamp_str}{original_path.suffix}"
    logger.info(f"Generated new file name: {new_file_name}")

    # 4) copy the file into the pipeline storage subdirectory
    #    and assign a temporary name identical to the original.
    copy_file_path: Path = move_file(str(original_path), str(pipeline_storage_subdir))

    # 5) Rename the moved file to include subdir + timestamp
    final_path: Path = rename_file(str(copy_file_path), new_file_name)
    logger.info(f"Reflected file into pipeline storage: {final_path}")


@log_exceptions_with_args
def process_file(file_path: str) -> None:
    """
    Processes a file through the pipeline with error handling and database mirroring.
    """

    if not Path(file_path).exists():
        logger.error(f"File does not exist: {file_path}")
        raise FileNotFoundError(f"The file {file_path} does not exist!")

    # Identify current directory and define subfolders
    current_dir_path = Path(file_path).parent
    logger.debug(f"process_file -> current_dir_path {current_dir_path}")
    working_dir = current_dir_path / "working"
    processed_dir = current_dir_path / "processed"
    error_dir = current_dir_path / "error"

    # Create subdirectories if needed
    working_dir.mkdir(exist_ok=True)
    processed_dir.mkdir(exist_ok=True)
    error_dir.mkdir(exist_ok=True)

    # Extract just the file name
    file_name = Path(file_path).name

    try:
        # 1) Copy the file into the "working" folder
        #    Pass only the folder to "copy_file"
        copy_file(file_path, str(working_dir))

        # 2) Build the file’s new path after copying
        working_file_path = working_dir / file_name

        # 3) Retrieve & execute the appropriate processor
        logger.debug(f"processing {file_name} in {working_file_path}")
        processor_module = get_processor_function(current_dir_path.name)   # look for module using the current step dir name
        logger.debug(f"found processor {processor_module}")

        # execute function from processor_module
        result = False
        if processor_module:
            if hasattr(processor_module, PROCESS_FILE_FUNCTION_NAME):
                # Retrieve the function object
                func_to_call = getattr(processor_module, PROCESS_FILE_FUNCTION_NAME)
                # Optionally, check if it is callable
                if callable(func_to_call):
                    try:
                        # Execute the function; pass any parameters as needed.
                        result = func_to_call(working_file_path)  # or func_to_call(args...) if parameters are required
                        processed_file_path = processed_dir / file_name # create the processed file path
                    except Exception as e:
                        print(f"An error occurred while executing {PROCESS_FILE_FUNCTION_NAME}: {e}")
                else:
                    print(f"Attribute {PROCESS_FILE_FUNCTION_NAME} exists but is not callable.")
            else:
                print(f"the pipeline process '{current_dir_path.name}' does not have a function named '{PROCESS_FILE_FUNCTION_NAME}'.")

        # 4) Reflect success/failure in pipeline storage
        reflect_to_pipeline_storage(str(current_dir_path), str(file_path), result) # copy step original to pipeline_storage
        reflect_to_pipeline_storage(str(current_dir_path), str(processed_file_path), result) # copy step result to pipeline_storage

        if result:
            # If processing succeeded, move to next or "processed" folder
            next_dir = get_next_dir(str(file_path))
            if next_dir:
                move_file(str(processed_file_path), str(next_dir))
            else:
                move_file(str(processed_file_path), str(processed_dir))
        else:
            # If processing failed, move to "error" folder (possibly renaming)
            error_file_path = str(error_dir / f"{file_name}.err")
            move_file(str(working_file_path), str(error_file_path))
            reflect_to_pipeline_storage(str(current_dir_path), str(processed_file_path),
                                        result)  # copy step result to pipeline_storage

    except Exception as e:
        # On any exception, log and move the original file to "error"
        logger.error(f"Unexpected error while processing file {file_path}: {e}")
        move_file(file_path, str(error_dir / f"{file_name}.err"))


@log_exceptions_with_args
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

@log_exceptions_with_args
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
