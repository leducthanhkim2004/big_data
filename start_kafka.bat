@echo off
echo Stopping any existing Kafka containers...
docker-compose down

echo.
echo Starting Kafka cluster with updated ports...
docker-compose up -d

echo.
echo Waiting for Kafka to be ready...
timeout /t 30 /nobreak

echo.
echo Kafka cluster should now be running!
echo.
echo Check status with: docker-compose ps
echo Monitor with: http://localhost:8080
echo.
echo You can now run:
echo   python simple_data_streamer.py --data-folder "D:\feb_to_apr"
echo.
pause 