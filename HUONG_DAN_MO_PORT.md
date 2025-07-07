# Hướng Dẫn Mở Port Để Nhiều Máy Kết Nối

## 🖥️ **IP của máy bạn:**
- **IP chính:** `172.16.131.75`
- **IP khác:** `192.168.56.1`, `192.168.197.1`, `192.168.222.1`

## 🔧 **Cách 1: Sử dụng Script (Khuyến nghị)**

### **Bước 1: Chạy script với quyền Administrator**

**Cách A: Sử dụng file .bat**
1. Chuột phải vào file `open_ports.bat`
2. Chọn "Run as administrator"
3. Nhấn "Yes" khi được hỏi

**Cách B: Sử dụng PowerShell**
1. Mở PowerShell với quyền Administrator
2. Chạy lệnh:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\open_ports.ps1
```

## 🔧 **Cách 2: Mở port thủ công**

### **Mở Windows Firewall:**

1. **Mở Windows Defender Firewall:**
   - Nhấn `Win + R`
   - Gõ `wf.msc`
   - Nhấn Enter

2. **Tạo rule mới:**
   - Click "Inbound Rules" ở bên trái
   - Click "New Rule..." ở bên phải
   - Chọn "Port"
   - Chọn "TCP" và nhập port cụ thể
   - Chọn "Allow the connection"
   - Chọn tất cả profiles (Domain, Private, Public)
   - Đặt tên rule

### **Ports cần mở:**
- **Port 5000** - Flask Server 1
- **Port 5001** - Flask Server 2  
- **Port 8080** - Load Balancer
- **Port 3001** - Client 1
- **Port 3002** - Client 2

## 🚀 **Cách 3: Sử dụng Command Line**

### **Mở Command Prompt với quyền Administrator:**
```cmd
# Mở port 5000
netsh advfirewall firewall add rule name="Flask Server 1" dir=in action=allow protocol=TCP localport=5000

# Mở port 5001
netsh advfirewall firewall add rule name="Flask Server 2" dir=in action=allow protocol=TCP localport=5001

# Mở port 8080
netsh advfirewall firewall add rule name="Load Balancer" dir=in action=allow protocol=TCP localport=8080

# Mở port 3001
netsh advfirewall firewall add rule name="Client 1" dir=in action=allow protocol=TCP localport=3001

# Mở port 3002
netsh advfirewall firewall add rule name="Client 2" dir=in action=allow protocol=TCP localport=3002
```

## 🧪 **Test kết nối**

### **Bước 1: Chạy servers**
```bash
# Terminal 1
python flask_s1.py

# Terminal 2
python flask_s2.py

# Terminal 3
python flask_lb.py
```

### **Bước 2: Test từ máy chủ**
- Mở trình duyệt
- Truy cập: `http://localhost:8080`

### **Bước 3: Test từ máy khác**
- Từ máy khác trong cùng mạng
- Truy cập: `http://172.16.131.75:8080`

## 🌐 **URLs để truy cập từ máy khác:**

```
Load Balancer: http://172.16.131.75:8080
Server 1:      http://172.16.131.75:5000
Server 2:      http://172.16.131.75:5001
Client 1:      http://172.16.131.75:3001
Client 2:      http://172.16.131.75:3002
```

## 🔍 **Kiểm tra port đã mở:**

```cmd
# Kiểm tra port đang listen
netstat -an | findstr :5000
netstat -an | findstr :5001
netstat -an | findstr :8080
```

## ⚠️ **Lưu ý quan trọng:**

1. **Bảo mật:** Chỉ mở port khi cần thiết
2. **Router:** Nếu kết nối qua router, có thể cần port forwarding
3. **Antivirus:** Một số antivirus có thể chặn kết nối
4. **Network:** Đảm bảo các máy trong cùng mạng

## 🛠️ **Troubleshooting:**

### **Nếu không kết nối được:**
1. Kiểm tra firewall đã mở chưa
2. Kiểm tra server đã chạy chưa
3. Thử ping IP: `ping 172.16.131.75`
4. Kiểm tra antivirus có chặn không

### **Nếu port bị chặn:**
```cmd
# Xóa rule cũ
netsh advfirewall firewall delete rule name="Flask Server 1"

# Tạo lại rule
netsh advfirewall firewall add rule name="Flask Server 1" dir=in action=allow protocol=TCP localport=5000
```

## 📱 **Test từ điện thoại:**
- Kết nối điện thoại vào cùng WiFi
- Mở trình duyệt
- Truy cập: `http://172.16.131.75:8080` 