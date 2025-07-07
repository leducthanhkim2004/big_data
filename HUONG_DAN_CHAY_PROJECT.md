# Hướng Dẫn Chạy Project Distributed File Storage System

## Mô tả Project
Đây là một hệ thống lưu trữ file phân tán sử dụng Flask với load balancer để phân phối tải giữa các server. Hệ thống cho phép upload và download file thông qua giao diện web.

## Cách 1: Chạy với Load Balancer (Khuyến nghị)

### Bước 1: Cài đặt dependencies
```bash
pip install flask requests werkzeug
```

### Bước 2: Tạo thư mục uploads (nếu chưa có)
```bash
mkdir uploads
```

### Bước 3: Chạy các Flask servers
Mở 3 terminal khác nhau và chạy lần lượt:

**Terminal 1 - Server 1 (Port 5000):**
```bash
python flask_s1.py
```

**Terminal 2 - Server 2 (Port 5001):**
```bash
python flask_s2.py
```

**Terminal 3 - Load Balancer (Port 8080):**
```bash
python flask_lb.py
```

### Bước 4: Truy cập ứng dụng
Mở trình duyệt và truy cập: `http://localhost:8080`

## Cách 2: Chạy với Client Applications

### Bước 1: Cài đặt dependencies
```bash
pip install flask requests werkzeug
```

### Bước 2: Chạy các Flask servers
Mở 2 terminal và chạy:

**Terminal 1 - Server 1 (Port 5000):**
```bash
python flask_s1.py
```

**Terminal 2 - Server 2 (Port 5001):**
```bash
python flask_s2.py
```

### Bước 3: Chạy Client applications
Mở 2 terminal khác và chạy:

**Terminal 3 - Client 1 (Port 3001):**
```bash
python client1.py
```

**Terminal 4 - Client 2 (Port 3002):**
```bash
python client2.py
```

### Bước 4: Truy cập ứng dụng
- Client 1: `http://localhost:3001`
- Client 2: `http://localhost:3002`

## Tính năng của hệ thống

### 1. Upload File
- Chọn file từ máy tính
- Click "Upload" để tải file lên server
- File sẽ được lưu với tên duy nhất (UUID + tên gốc)

### 2. Download File
- Chọn file từ dropdown menu
- Click "Download" để tải file về máy

### 3. Load Balancing
- Load balancer tự động phân phối request giữa 2 server
- Server có tải thấp hơn sẽ được ưu tiên xử lý request

### 4. Fault Tolerance
- Nếu một server bị lỗi, request sẽ được chuyển sang server khác
- Đảm bảo hệ thống hoạt động liên tục

## Cấu trúc thư mục
```
Distributed-File-Storage-Systems-main/
├── flask_lb.py          # Load balancer
├── flask_s1.py          # Flask server 1 (port 5000)
├── flask_s2.py          # Flask server 2 (port 5001)
├── client1.py           # Client application 1 (port 3001)
├── client2.py           # Client application 2 (port 3002)
├── templates/
│   └── index.html       # Giao diện web
├── uploads/             # Thư mục lưu file
└── README.md
```

## Lưu ý quan trọng
1. Đảm bảo tất cả các port (5000, 5001, 8080, 3001, 3002) không bị sử dụng bởi ứng dụng khác
2. Thư mục `uploads` phải tồn tại trước khi chạy server
3. Cần cài đặt Python và pip trước khi chạy project
4. Nếu gặp lỗi "ModuleNotFoundError", hãy cài đặt thêm thư viện còn thiếu

## Troubleshooting
- **Lỗi "Address already in use"**: Đóng ứng dụng đang sử dụng port đó hoặc thay đổi port trong code
- **Lỗi "No module named 'flask'"**: Chạy `pip install flask`
- **Lỗi "No module named 'requests'"**: Chạy `pip install requests` 