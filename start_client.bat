@echo off
echo Client Machine Data Receiver
echo ============================

echo.
echo Please enter your machine ID (e.g., machine1, machine2, laptop1):
set /p MACHINE_ID=

echo.
echo Please enter the main machine IP address:
set /p MAIN_IP=

echo.
echo Starting data receiver for machine: %MACHINE_ID%
echo Connecting to: %MAIN_IP%
echo.

python simple_data_consumer.py --machine-id %MACHINE_ID% --brokers %MAIN_IP%:29092 %MAIN_IP%:29093 %MAIN_IP%:29094

echo.
echo Data receiver stopped.
echo Check the received_blocks_%MACHINE_ID% folder for received data.
pause 