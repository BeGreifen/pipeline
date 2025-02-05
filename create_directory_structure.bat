@echo off

:: Create 'pipeline' directory and its subfolders
mkdir pipeline
mkdir pipeline\10_example_step_one
mkdir pipeline\20_example_step_two

:: Create 'database' directory and its subfolders
mkdir database
mkdir database\10_example_step_one
mkdir database\20_example_step_two

:: Create 'processes' directory 
mkdir processes

:: Create Python files in 'processes' directory with pipeline prefix from config (e.g. pipeline_step_)
echo # Placeholder Python script > processes\pipeline_step_10_example_step_one.py
echo # Placeholder Python script > processes\pipeline_step_20_example_step_two.py

:: Create the 'logs' directory
mkdir logs

:: Create a top-level 'Pipeline' directory with a '99_success' subfolder
mkdir Pipeline
mkdir Pipeline\99_success

:: Create a 'Processed' directory for error files
mkdir Processed
mkdir Processed\Error

:: Create 'Pipeline_Storage'
mkdir Pipeline_Storage

echo Directory structure created successfully!
pause