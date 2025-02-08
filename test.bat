@echo off

rem Remove the .\pipeline\10_example_step_one\ directory (if it exists)
if exist ".\pipeline\10_example_step_one" (
    rmdir /s /q ".\pipeline\10_example_step_one"
)
rem Remove the .\pipeline\20_example_step_one\ directory (if it exists)
if exist ".\pipeline\20_example_step_two" (
    rmdir /s /q ".\pipeline\20_example_step_two"
)
rem Remove the .\pipeline\99_success\ directory (if it exists)
if exist ".\pipeline\99_success" (
    rmdir /s /q ".\pipeline\99_success"
)


rem Create the pipeline directories
mkdir ".\pipeline\10_example_step_one"
mkdir ".\pipeline\20_example_step_two"
mkdir ".\pipeline\99_success"

rem Remove the .\Pipeline_Storage\ directory (if it exists)
if exist ".\Pipeline_Storage" (
    rmdir /s /q ".\Pipeline_Storage"
)

rem Create the .\Pipeline_Storage\ directory
:: mkdir ".\Pipeline_Storage"

