:: This batch file sets up a skeleton directory structure for a multi-step pipeline.
:: It creates the necessary folders for pipeline steps, database storage, process scripts,
:: logs, and separate areas for completed work and error handling.
:: Running this file ensures the environment is ready for all project-related operations.

@echo off

:: Create 'pipeline' directory and its subfolders
mkdir pipeline
mkdir pipeline\10_example_step_one
mkdir pipeline\20_example_step_two
mkdir Pipeline\99_success

:: Create 'pipeline_storeage' directory and its subfolders
mkdir pipeline_storage
mkdir pipeline_storage\10_example_step_one
mkdir pipeline_storage\20_example_step_two

:: Create 'processes' directory 
mkdir processes

:: Create Python files in 'processes' directory with pipeline prefix from config (e.g. pipeline_step_)
echo # Placeholder Python script > processes\pipeline_step_10_example_step_one.py
echo # Placeholder Python script > processes\pipeline_step_20_example_step_two.py

:: Create the 'logs' directory
mkdir logs

:: Create a 'Processed' directory for error files
mkdir Processed
mkdir Processed\Error

echo Directory structure created successfully!
timeout /t 10 >nul
