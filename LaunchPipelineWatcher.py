"""
Main script to launch the pipeline file watcher for monitoring and processing new files.
"""

from utils.pipeline_file_watcher import PipelineFileWatcher


def main() -> None:
    """
    Main entry point that instantiates and runs the pipeline file watcher.
    """
    watcher = PipelineFileWatcher()
    watcher.logger.info("Starting the Pipeline File Watcher...")
    watcher.run()


if __name__ == "__main__":
    main()