"""
Main script to launch the pipeline file watcher for monitoring and processing new files.
"""
import logging
from pathlib import Path  # Simplifies file path operations
from utils.pipeline_file_watcher import PipelineFileWatcher
from setup import config_setup  # Interfaces with config.ini functionalities
import setup.logging_setup as logging_setup  # Manages logging configuration

# Dynamically obtain the logger name from the script name (without extension).
config = config_setup.get_prod_config()
script_name: str = Path(__file__).stem


# Build the absolute path for the log file
logs_dir: Path = logging_setup.configure_logs_directory()
logfile_path = logs_dir / f"{script_name}.log"


# Get the logger instance
logger = logging_setup.get_logger(
    logger_name=script_name,
    logfile_name=logfile_path,
    console_level=logging.INFO,
    file_level=logging.DEBUG
)

def main() -> None:
    """
    Main entry point that instantiates and runs the pipeline file watcher.
    """
    logger.info("Launching the Pipeline File Watcher...")
    watcher = PipelineFileWatcher()
    watcher.logger.info("Starting the Pipeline File Watcher...")
    watcher.run()


if __name__ == "__main__":
    main()