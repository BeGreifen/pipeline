@echo off

echo Creating 100 text files in .\pipeline\10_example_step_one\
for /l %%i in (1,1,50) do (
    echo This is file %%i > ".\pipeline\10_example_step_one\text%%i.txt"
    echo Created text%%i.txt
    timeout /t 1 /nobreak >nul
)

echo Done!

