import pytest
from pathlib import Path
from utils.file_ops import copy_file, rename_file, create_directory
from utils.pipeline_handling import reflect_to_pipeline_storage


# GLOBAL FIXTURE
@pytest.fixture
def temp_pipeline_structure(tmp_path):
    """
    Creates a temporary pipeline folder structure for testing:
    - Pipeline/
    - Pipeline_Storage/
    """
    base_dir = tmp_path / "pipeline"
    base_dir.mkdir()

    pipeline_storage_dir = tmp_path / "pipeline_storage"
    pipeline_storage_dir.mkdir()

    step_folder = base_dir / "10_raw_pdf"
    step_folder.mkdir()

    file_to_reflect = step_folder / "test_file.txt"
    file_to_reflect.write_text("Test data for processing.")

    return {
        "step_folder": step_folder,
        "file_to_reflect": file_to_reflect,
        "pipeline_storage_dir": pipeline_storage_dir,
    }


# TEST CASES
def test_reflect_success_file(temp_pipeline_structure, monkeypatch):
    """
    Test reflecting a successfully processed file into pipeline storage.
    """
    # Unpack the test structure
    step_folder = temp_pipeline_structure["step_folder"]
    file_to_reflect = temp_pipeline_structure["file_to_reflect"]
    pipeline_storage_dir = temp_pipeline_structure["pipeline_storage_dir"]

    # Mock the PIPELINE_STORAGE_DIR for testing
    monkeypatch.setattr("utils.pipeline_handling.PIPELINE_STORAGE_DIR", str(pipeline_storage_dir))

    # Call the reflect_to_pipeline_storage function (success case)
    reflect_to_pipeline_storage(str(step_folder), str(file_to_reflect), result=True)

    # Verify the file was copied to the correct storage location
    reflected_file = pipeline_storage_dir / "10_raw_pdf" / file_to_reflect.name
    assert reflected_file.exists()

    # FIXED: Include the period at the end
    assert reflected_file.read_text() == "Test data for processing."



def test_reflect_error_file(temp_pipeline_structure, monkeypatch):
    """
    Test reflecting a failed processing file into pipeline storage with '_causing_error'.
    """
    # Unpack the test structure
    step_folder = temp_pipeline_structure["step_folder"]
    file_to_reflect = temp_pipeline_structure["file_to_reflect"]
    pipeline_storage_dir = temp_pipeline_structure["pipeline_storage_dir"]

    # Mock the PIPELINE_STORAGE_DIR for testing
    monkeypatch.setattr("utils.pipeline_handling.PIPELINE_STORAGE_DIR", str(pipeline_storage_dir))

    # Call the reflect_to_pipeline_storage function (failure case)
    reflect_to_pipeline_storage(str(step_folder), str(file_to_reflect), result=False)

    # Verify the error file was copied and renamed correctly in storage
    reflected_error_file = pipeline_storage_dir / "10_raw_pdf" / f"{file_to_reflect.stem}_causing_error{file_to_reflect.suffix}"
    assert reflected_error_file.exists()

    # Correct assertion with a period at the end
    assert reflected_error_file.read_text() == "Test data for processing."



    # Verify the error file was copied and renamed correctly in storage
    reflected_error_file = pipeline_storage_dir / "10_raw_pdf" / f"{file_to_reflect.stem}_causing_error{file_to_reflect.suffix}"
    assert reflected_error_file.exists()
    