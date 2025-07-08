# Network Configuration for Distributed File Storage System
# Cấu hình để cho phép nhiều máy truy cập

# Server configurations
SERVER_CONFIGS = {
    'server1': {
        'host': '0.0.0.0',  # Cho phép tất cả IP truy cập
        'port': 5000,
        'name': 'Server-1'
    },
    'server2': {
        'host': '0.0.0.0',
        'port': 5001,
        'name': 'Server-2'
    },
    'server3': {
        'host': '0.0.0.0',
        'port': 5002,
        'name': 'Server-3'
    }
}

# Load Balancer configuration
LOAD_BALANCER_CONFIG = {
    'host': '0.0.0.0',
    'port': 8080
}

# Client configurations
CLIENT_CONFIGS = {
    'client1': {
        'host': '0.0.0.0',
        'port': 3001
    },
    'client2': {
        'host': '0.0.0.0',
        'port': 3002
    }
}

# Network discovery
def get_server_urls(base_ip='localhost'):
    """Generate server URLs for different machines"""
    return [
        f"http://{base_ip}:5000",
        f"http://{base_ip}:5001",
        f"http://{base_ip}:5002"
    ]

def get_load_balancer_url(base_ip='localhost'):
    """Get load balancer URL"""
    return f"http://{base_ip}:8080" 