import pytest
from pathlib import Path
from utils.file_ops import create_directory, move_file, copy_file, rename_file
from utils.pipeline_handling import create_working_dir


# GLOBAL FIXTURES
@pytest.fixture
def temp_test_structure(tmp_path):
    """
    Creates a temporary test folder structure using the pytest `tmp_path` fixture.
    This structure mimics a sample pipeline scenario.
    """
    base_dir = tmp_path / "pipeline"
    base_dir.mkdir()

    file_to_move = base_dir / "test_file.txt"
    file_to_move.write_text("This is a test file!")

    return {
        "base_dir": base_dir,
        "file_to_move": file_to_move,
    }


# TESTS FOR file_ops.py


def test_create_directory(temp_test_structure):
    """
    Test creating a directory using the create_directory() function.
    """
    base_dir = temp_test_structure["base_dir"]

    # Create a new subdirectory
    new_dir = base_dir / "new_folder"
    created_dir = create_directory(str(new_dir))

    # Assert the directory was created successfully
    assert created_dir.exists()
    assert created_dir.is_dir()


def test_move_file(temp_test_structure):
    """
    Test moving a file using the move_file() function.
    """
    base_dir = temp_test_structure["base_dir"]
    file_to_move = temp_test_structure["file_to_move"]

    # Destination folder
    destination_folder = base_dir / "destination"
    destination_folder.mkdir()

    # Move the file
    moved_file = move_file(str(file_to_move), str(destination_folder))

    # Assert the file was moved successfully
    assert moved_file.exists()
    assert not file_to_move.exists()
    assert moved_file.parent == destination_folder


def test_copy_file(temp_test_structure):
    """
    Test copying a file using the copy_file() function.
    """
    base_dir = temp_test_structure["base_dir"]
    file_to_copy = temp_test_structure["file_to_move"]

    # Destination folder
    destination_folder = base_dir / "copied_files"
    destination_folder.mkdir()

    # Copy the file
    copied_file = copy_file(str(file_to_copy), str(destination_folder))

    # Assert the file was copied successfully
    assert copied_file.exists()
    assert file_to_copy.exists()  # Original file shouldn't be removed
    assert copied_file.parent == destination_folder


def test_rename_file(temp_test_structure):
    """
    Test renaming a file using the rename_file() function.
    """
    file_to_rename = temp_test_structure["file_to_move"]

    # Rename the file
    new_name = "renamed_test_file.txt"
    renamed_file = rename_file(str(file_to_rename), new_name)

    # Assert the file was renamed successfully
    assert renamed_file.exists()
    assert not file_to_rename.exists()
    assert renamed_file.name == new_name


# TESTS FOR pipeline_handling.py


def test_create_working_dir(temp_test_structure):
    """
    Test creating a working directory using create_working_dir().
    """
    folder_path = temp_test_structure["base_dir"]

    # Create the working directory
    working_dir = create_working_dir(str(folder_path))

    # Assert the "working" directory was created successfully
    assert Path(working_dir).exists()
    assert Path(working_dir).name == "working"


def test_pipeline_flow_with_moving(temp_test_structure):
    """
    Test a small pipeline flow using `create_working_dir` and `move_file`.
    """
    base_dir = temp_test_structure["base_dir"]
    file_to_process = temp_test_structure["file_to_move"]

    # Simulate pipeline flow
    working_dir = create_working_dir(str(base_dir))
    moved_file = move_file(str(file_to_process), str(working_dir))

    # Assert the file is now in the working directory
    assert Path(moved_file).exists()
    assert Path(file_to_process).exists() is False
    assert Path(moved_file).parent == Path(working_dir)


def test_logging_in_file_ops(temp_test_structure, caplog):
    """
    Test if logging works correctly in `file_ops` operations.
    """
    # Use caplog to capture log messages
    base_dir = temp_test_structure["base_dir"]
    file_to_move = temp_test_structure["file_to_move"]
    destination_folder = base_dir / "logged_destination"

    # Perform file move operation (should log info)
    with caplog.at_level("INFO"):
        move_file(str(file_to_move), str(destination_folder))

    # Verify an info-level log was recorded for this operation
    assert "Moved file" in caplog.text


# TESTS FOR ADDED FUNCTIONALITY

@pytest.mark.parametrize("file_name", ["test1.txt", "test2.csv", "test3.json"])
def test_rename_and_move_file(file_name, tmp_path):
    """
    Comprehensive test to validate rename and move functionalities in one flow.
    """
    # Create initial setup
    base_dir = tmp_path / "pipeline"
    base_dir.mkdir()
    source_file = base_dir / file_name
    source_file.write_text("Content for rename test!")

    destination_folder = base_dir / "next_step"
    destination_folder.mkdir()

    # Rename and move the file
    renamed_file = rename_file(str(source_file), f"renamed_{file_name}")
    moved_file = move_file(str(renamed_file), str(destination_folder))

    # Assertions
    assert Path(moved_file).exists()
    assert Path(moved_file).parent == destination_folder
    assert "renamed_" in Path(moved_file).name


