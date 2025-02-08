"""
Module providing a file watcher (PipelineFileWatcher) that monitors specified
directories in a pipeline context for new files and delegates them for processing.
"""

import time
import threading
import logging
from pathlib import Path
from typing import Set, List

# Local imports (adjust as needed for your project)
from setup import config_setup
import setup.logging_setup as logging_setup
from utils import pipeline_handling


class PipelineFileWatcher:
    """
    Monitors pipeline directories for newly arrived files and delegates them
    to the pipeline handling logic for further processing.
    """

    def __init__(self) -> None:
        """
        Initializes the PipelineFileWatcher by reading configuration settings
        and setting up a dedicated logger.
        """
        self.config_settings = config_setup.get_prod_config()

        # Configure a logger for this watcher
        script_name: str = Path(__file__).stem
        logs_dir: Path = logging_setup.configure_logs_directory()
        logfile_path: Path = logs_dir / f"{script_name}.log"

        self.logger = logging_setup.get_logger(
            logger_name=script_name,
            logfile_name=str(logfile_path),
            console_level=logging.INFO,
            file_level=logging.DEBUG,
        )

    def run(self) -> None:
        """
        Launches a thread per subfolder in the base pipeline directory
        for continuous monitoring. Subfolders are watched concurrently,
        although files within the same folder are processed sequentially.
        """
        poll_interval = self.config_settings["PIPELINE"].getint("poll_frequency", fallback=30)
        pipeline_directory = Path(self.config_settings["PIPELINE"].get("pipeline_dir", ".")).resolve()

        self.logger.info(f"Base directory for pipeline: {pipeline_directory}")
        self.logger.info(f"Polling frequency: {poll_interval} seconds")

        subfolders = [
            folder for folder in pipeline_directory.iterdir()
            if folder.is_dir() and not folder.name.startswith(".")
        ]

        for directory_path in subfolders:
            thread = threading.Thread(
                target=self._monitor_subfolder,
                args=(directory_path, poll_interval),
                daemon=True
            )
            thread.start()
            self.logger.info(f"Started monitoring thread for: {directory_path}")

        # Keep the main thread alive indefinitely
        while True:
            time.sleep(3600)

    def _monitor_subfolder(self, directory_path: Path, poll_interval: int) -> None:
        """
        Monitors a single subfolder for new files at a fixed poll interval.
        Any new file found is processed.

        Args:
            directory_path (Path): Path to the directory being monitored.
            poll_interval (int): Frequency (in seconds) to check for new files.
        """
        known_files: Set[Path] = set(directory_path.iterdir())

        while True:
            try:
                current_files = set(directory_path.iterdir())
                new_files: List[Path] = [f for f in current_files - known_files if f.is_file()]

                for file_path in new_files:
                    self.logger.info(f"New file detected: {file_path}")
                    self._process_file_safely(file_path)

                known_files = current_files
            except Exception:
                self.logger.exception(f"Error monitoring {directory_path}")
            finally:
                time.sleep(poll_interval)

    def _process_file_safely(self, file_path: Path) -> None:
        """
        Safely processes a newly detected file by calling pipeline handling logic.
        Logs any exceptions rather than halting the entire script.

        Args:
            file_path (Path): Path to the new file.
        """
        try:
            pipeline_handling.process_file(str(file_path))
            self.logger.info(f"Successfully processed file: {file_path}")
        except FileNotFoundError:
            self.logger.exception(f"File not found: {file_path}")
        except Exception:
            self.logger.exception(f"Error processing file: {file_path}")