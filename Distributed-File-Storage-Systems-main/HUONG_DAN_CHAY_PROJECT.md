# Project Setup Guide - Distributed File Storage System

## Project Description
This is a distributed file storage system using Flask with load balancer to distribute load between servers. The system allows file upload and download through web interface.

## Method 1: Run with Load Balancer (Recommended)

### Step 1: Install dependencies
```bash
pip install flask requests werkzeug
```

### Step 2: Create uploads directory (if not exists)
```bash
mkdir uploads
```

### Step 3: Run Flask servers
Open 3 different terminals and run:

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

### Step 4: Access application
Open browser and access: `http://localhost:8080`

## Method 2: Run with Client Applications

### Step 1: Install dependencies
```bash
pip install flask requests werkzeug
```

### Step 2: Run Flask servers
Open 2 terminals and run:

**Terminal 1 - Server 1 (Port 5000):**
```bash
python flask_s1.py
```

**Terminal 2 - Server 2 (Port 5001):**
```bash
python flask_s2.py
```

### Step 3: Run Client applications
Open 2 more terminals and run:

**Terminal 3 - Client 1 (Port 3001):**
```bash
python client1.py
```

**Terminal 4 - Client 2 (Port 3002):**
```bash
python client2.py
```

### Step 4: Access application
- Client 1: `http://localhost:3001`
- Client 2: `http://localhost:3002`

## System Features

### 1. Upload File
- Select file from computer
- Click "Upload" to upload file to server
- File will be saved with unique name (UUID + original name)

### 2. Download File
- Select file from dropdown menu
- Click "Download" to download file to computer

### 3. Load Balancing
- Load balancer automatically distributes requests between 2 servers
- Server with lower load will be prioritized to handle requests

### 4. Fault Tolerance
- If one server fails, requests will be redirected to other server
- Ensures continuous system operation

## Directory Structure
```
Distributed-File-Storage-Systems-main/
├── flask_lb.py          # Load balancer
├── flask_s1.py          # Flask server 1 (port 5000)
├── flask_s2.py          # Flask server 2 (port 5001)
├── client1.py           # Client application 1 (port 3001)
├── client2.py           # Client application 2 (port 3002)
├── templates/
│   └── index.html       # Web interface
├── uploads/             # File storage directory
└── README.md
```

## Important Notes
1. Ensure all ports (5000, 5001, 8080, 3001, 3002) are not used by other applications
2. The `uploads` directory must exist before running server
3. Python and pip must be installed before running project
4. If you encounter "ModuleNotFoundError", install the missing library

## Troubleshooting
- **Error "Address already in use"**: Close application using that port or change port in code
- **Error "No module named 'flask'"**: Run `pip install flask`
- **Error "No module named 'requests'"**: Run `pip install requests` 