@echo off
echo Opening ports for Distributed File Storage System...
echo This script needs to be run as Administrator

REM Open Flask Server 1 (Port 5000)
netsh advfirewall firewall add rule name="Flask Server 1" dir=in action=allow protocol=TCP localport=5000
echo Port 5000 opened for Flask Server 1

REM Open Flask Server 2 (Port 5001)
netsh advfirewall firewall add rule name="Flask Server 2" dir=in action=allow protocol=TCP localport=5001
echo Port 5001 opened for Flask Server 2

REM Open Load Balancer (Port 8080)
netsh advfirewall firewall add rule name="Load Balancer" dir=in action=allow protocol=TCP localport=8080
echo Port 8080 opened for Load Balancer

REM Open Client 1 (Port 3001)
netsh advfirewall firewall add rule name="Client 1" dir=in action=allow protocol=TCP localport=3001
echo Port 3001 opened for Client 1

REM Open Client 2 (Port 3002)
netsh advfirewall firewall add rule name="Client 2" dir=in action=allow protocol=TCP localport=3002
echo Port 3002 opened for Client 2

echo.
echo All ports opened successfully!
echo.
echo To access from other machines, use your computer's IP address:
echo - Load Balancer: http://YOUR_IP:8080
echo - Server 1: http://YOUR_IP:5000
echo - Server 2: http://YOUR_IP:5001
echo - Client 1: http://YOUR_IP:3001
echo - Client 2: http://YOUR_IP:3002
echo.
pause 