# LaunchPipelineDashboard.py

import asyncio
import logging
from pathlib import Path  # Simplifies file path operations
from utils.pipeline_file_watcher import PipelineFileWatcher
from setup import config_setup
import setup.logging_setup as logging_setup
from pipeline_dashboard_websocket import PipelineStatus  # Adjust the import as needed

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


async def start_dashboard_server() -> None:
    """
    Start the PipelineDashboard WebSocket server.
    """
    dashboard = PipelineDashboard()
    logger.info("Starting the Pipeline Dashboard WebSocket server...")
    await dashboard.run_server()


async def start_file_watcher() -> None:
    """
    Run the PipelineFileWatcher.
    If PipelineFileWatcher.run() is blocking, run it in an executor.
    """
    watcher = PipelineFileWatcher()
    logger.info("Starting the Pipeline File Watcher...")

    loop = asyncio.get_running_loop()
    # Running the file watcher in a thread to avoid blocking async loop
    await loop.run_in_executor(None, watcher.run)


async def main() -> None:
    """
    Main async entry point: start both dashboard and file watcher concurrently.
    """
    await asyncio.gather(
        start_dashboard_server(),
        start_file_watcher()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error("Error while running the system: %s", e)
