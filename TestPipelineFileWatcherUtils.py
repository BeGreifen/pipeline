# tests/test_pipeline_file_watcher.py

"""
Unit tests for the PipelineFileWatcher class using pytest.

These tests check whether new files are correctly handed off to pipeline_handling
without causing unexpected exceptions or failures.
"""

import pytest
from unittest.mock import patch
from pathlib import Path

from utils.pipeline_file_watcher import PipelineFileWatcher


@pytest.mark.parametrize("file_contents", ["", "Some data", "123"])
@patch("utils.pipeline_file_watcher.pipeline_handling.process_file")
def test_process_file_safely(mock_process_file, file_contents, tmp_path):
    """
    Tests that _process_file_safely calls the pipeline's process_file correctly.

    Args:
        mock_process_file (MagicMock): Mocked version of pipeline_handling.process_file.
        file_contents (str): Parametrized content to write to the file for testing.
        tmp_path (Path): Pytest fixture for creating a temporary directory.
    """
    # Create the PipelineFileWatcher instance
    watcher = PipelineFileWatcher()

    # Create a temporary file where 'file_contents' is written
    file_path: Path = tmp_path / "test_file.txt"
    file_path.write_text(file_contents)

    # Call the private file-processing method
    watcher._process_file_safely(file_path)

    # The pipeline_handling.process_file function should have been called exactly once.
    mock_process_file.assert_called_once_with(str(file_path))


@patch("utils.pipeline_file_watcher.pipeline_handling.process_file", side_effect=FileNotFoundError)
def test_process_file_not_found(mock_process_file, tmp_path):
    """
    Ensures that a FileNotFoundError raised inside _process_file_safely is caught
    and logged, without crashing the watcher.

    Args:
        mock_process_file (MagicMock): Mocked version of pipeline_handling.process_file.
        tmp_path (Path): Pytest fixture for creating a temporary directory.
    """
    watcher = PipelineFileWatcher()
    missing_file_path: Path = tmp_path / "non_existent_file.txt"

    # Even though the file doesn't exist, _process_file_safely should not raise; it should log instead.
    watcher._process_file_safely(missing_file_path)

    mock_process_file.assert_called_once_with(str(missing_file_path))