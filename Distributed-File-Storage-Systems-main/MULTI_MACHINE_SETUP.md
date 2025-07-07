# Hướng Dẫn Triển Khai Multi-Machine

## 🖥️ **Cấu hình cho nhiều máy truy cập**

### **Bước 1: Tìm IP của máy chủ**
```bash
# Windows
ipconfig

# Linux/Mac
ifconfig
# hoặc
ip addr show
```

### **Bước 2: Cấu hình Firewall**
Mở các port cần thiết:

**Windows:**
```bash
# Mở Command Prompt với quyền Administrator
netsh advfirewall firewall add rule name="Flask Server 1" dir=in action=allow protocol=TCP localport=5000
netsh advfirewall firewall add rule name="Flask Server 2" dir=in action=allow protocol=TCP localport=5001
netsh advfirewall firewall add rule name="Load Balancer" dir=in action=allow protocol=TCP localport=8080
netsh advfirewall firewall add rule name="Client 1" dir=in action=allow protocol=TCP localport=3001
netsh advfirewall firewall add rule name="Client 2" dir=in action=allow protocol=TCP localport=3002
```

**Linux:**
```bash
sudo ufw allow 5000
sudo ufw allow 5001
sudo ufw allow 8080
sudo ufw allow 3001
sudo ufw allow 3002
```

### **Bước 3: Chạy servers trên máy chủ**
```bash
# Terminal 1 - Server 1
python flask_s1.py

# Terminal 2 - Server 2  
python flask_s2.py

# Terminal 3 - Load Balancer
python flask_lb.py

# Terminal 4 - Client 1 (optional)
python client1.py

# Terminal 5 - Client 2 (optional)
python client2.py
```

### **Bước 4: Truy cập từ máy khác**
Thay `YOUR_SERVER_IP` bằng IP thực của máy chủ:

- **Load Balancer:** `http://YOUR_SERVER_IP:8080`
- **Server 1 trực tiếp:** `http://YOUR_SERVER_IP:5000`
- **Server 2 trực tiếp:** `http://YOUR_SERVER_IP:5001`
- **Client 1:** `http://YOUR_SERVER_IP:3001`
- **Client 2:** `http://YOUR_SERVER_IP:3002`

## 🌐 **Triển khai trên Internet (Cloud)**

### **Option 1: AWS EC2**
```bash
# 1. Tạo EC2 instance
# 2. Cấu hình Security Groups (mở port 8080, 5000, 5001)
# 3. SSH vào instance
ssh -i your-key.pem ubuntu@your-ec2-ip

# 4. Cài đặt Python và dependencies
sudo apt update
sudo apt install python3 python3-pip
pip3 install flask requests werkzeug

# 5. Upload code và chạy
python3 flask_s1.py &
python3 flask_s2.py &
python3 flask_lb.py &
```

### **Option 2: Google Cloud Platform**
```bash
# Tương tự AWS, nhưng sử dụng Google Cloud Console
# Mở port trong Firewall Rules
```

### **Option 3: Heroku**
Tạo file `Procfile`:
```
web: python flask_lb.py
```

## 🔧 **Cấu hình nâng cao**

### **1. Sử dụng Nginx làm Reverse Proxy**
```nginx
# /etc/nginx/sites-available/distributed-storage
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### **2. Sử dụng PM2 để quản lý processes**
```bash
npm install -g pm2
pm2 start flask_s1.py --name "server1" --interpreter python
pm2 start flask_s2.py --name "server2" --interpreter python
pm2 start flask_lb.py --name "loadbalancer" --interpreter python
pm2 save
pm2 startup
```

### **3. Sử dụng Docker**
Tạo `Dockerfile`:
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["python", "flask_lb.py"]
```

## 📊 **Monitoring và Logging**

### **Tạo monitoring script:**
```python
# monitor.py
import requests
import time
import json

def check_server_health():
    servers = [
        "http://localhost:5000",
        "http://localhost:5001"
    ]
    
    status = {}
    for server in servers:
        try:
            response = requests.get(server, timeout=5)
            status[server] = "UP" if response.status_code == 200 else "DOWN"
        except:
            status[server] = "DOWN"
    
    return status

# Chạy monitoring
while True:
    print(json.dumps(check_server_health(), indent=2))
    time.sleep(30)
```

## 🔒 **Bảo mật**

### **1. Sử dụng HTTPS**
```bash
# Tạo SSL certificate
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes
```

### **2. Authentication**
Thêm basic auth vào Flask:
```python
from functools import wraps
from flask import request, Response

def check_auth(username, password):
    return username == 'admin' and password == 'secret'

def authenticate():
    return Response('Could not verify your access level for that URL.\n'
                   'You have to login with proper credentials', 401,
                   {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated
```

## 🚀 **Scaling**

### **Horizontal Scaling:**
1. Thêm nhiều server instances
2. Sử dụng load balancer (HAProxy, Nginx)
3. Implement service discovery

### **Vertical Scaling:**
1. Tăng RAM và CPU cho server
2. Optimize code performance
3. Sử dụng caching (Redis)

## 📝 **Checklist triển khai**

- [ ] Cấu hình network (host='0.0.0.0')
- [ ] Mở firewall ports
- [ ] Test local access
- [ ] Test remote access
- [ ] Cấu hình SSL (nếu cần)
- [ ] Setup monitoring
- [ ] Backup strategy
- [ ] Documentation 