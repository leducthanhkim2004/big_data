# PowerShell script to open ports for Distributed File Storage System
# Run as Administrator

Write-Host "Opening ports for Distributed File Storage System..." -ForegroundColor Green
Write-Host "This script needs to be run as Administrator" -ForegroundColor Yellow

# Open Flask Server 1 (Port 5000)
New-NetFirewallRule -DisplayName "Flask Server 1" -Direction Inbound -Protocol TCP -LocalPort 5000 -Action Allow
Write-Host "Port 5000 opened for Flask Server 1" -ForegroundColor Green

# Open Flask Server 2 (Port 5001)
New-NetFirewallRule -DisplayName "Flask Server 2" -Direction Inbound -Protocol TCP -LocalPort 5001 -Action Allow
Write-Host "Port 5001 opened for Flask Server 2" -ForegroundColor Green

# Open Load Balancer (Port 8080)
New-NetFirewallRule -DisplayName "Load Balancer" -Direction Inbound -Protocol TCP -LocalPort 8080 -Action Allow
Write-Host "Port 8080 opened for Load Balancer" -ForegroundColor Green

# Open Client 1 (Port 3001)
New-NetFirewallRule -DisplayName "Client 1" -Direction Inbound -Protocol TCP -LocalPort 3001 -Action Allow
Write-Host "Port 3001 opened for Client 1" -ForegroundColor Green

# Open Client 2 (Port 3002)
New-NetFirewallRule -DisplayName "Client 2" -Direction Inbound -Protocol TCP -LocalPort 3002 -Action Allow
Write-Host "Port 3002 opened for Client 2" -ForegroundColor Green

Write-Host ""
Write-Host "All ports opened successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "To access from other machines, use your computer's IP address:" -ForegroundColor Cyan
Write-Host "- Load Balancer: http://YOUR_IP:8080" -ForegroundColor White
Write-Host "- Server 1: http://YOUR_IP:5000" -ForegroundColor White
Write-Host "- Server 2: http://YOUR_IP:5001" -ForegroundColor White
Write-Host "- Client 1: http://YOUR_IP:3001" -ForegroundColor White
Write-Host "- Client 2: http://YOUR_IP:3002" -ForegroundColor White
Write-Host ""

# Get IP address
$ip = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object {$_.IPAddress -like "192.168.*" -or $_.IPAddress -like "10.*" -or $_.IPAddress -like "172.*"}).IPAddress
if ($ip) {
    Write-Host "Your local IP address: $ip" -ForegroundColor Yellow
    Write-Host "Example: http://$ip`:8080" -ForegroundColor Yellow
}

Read-Host "Press Enter to continue" 