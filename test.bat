@echo off

rem Remove the .\pipeline\10_example_step_one\ directory (if it exists)
if exist ".\pipeline\10_example_step_one" (
    rmdir /s /q ".\pipeline\10_example_step_one"
)

rem Create the .\pipeline\10_example_step_one\ directory
mkdir ".\pipeline\10_example_step_one"


rem Remove the .\pipeline\10_example_step_one\ directory again
if exist ".\pipeline\10_example_step_one" (
    rmdir /s /q ".\pipeline\10_example_step_one"
)

rem Create the .\pipeline\10_example_step_one\ directory again
mkdir ".\pipeline\10_example_step_one"


rem Remove the .\Pipeline_Storage\ directory (if it exists)
if exist ".\Pipeline_Storage" (
    rmdir /s /q ".\Pipeline_Storage"
)

rem Create the .\Pipeline_Storage\ directory
mkdir ".\Pipeline_Storage"

rem Wait 2 seconds
timeout /t 2 /nobreak >nul

rem Copy test.txt to the .\pipeline\10_example_step_one\ directory
copy "test.txt" ".\pipeline\10_example_step_one\"
