from flask import Flask, request, send_file, redirect, url_for
import os
from werkzeug.utils import secure_filename
import uuid
from hdfs_features import HDFSFeatures
from block_manager import BlockManager

UPLOAD_FOLDER = 'uploads'
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Try to connect to gRPC server for active client management
try:
    import grpc
    import task_pb2_grpc
    
    channel = grpc.insecure_channel('localhost:50051')
    stub = task_pb2_grpc.TaskServiceStub(channel)
    
    # Create a simple wrapper to get active clients
    class ActiveClientManager:
        def __init__(self, stub):
            self.stub = stub
        
        def get_active_clients(self):
            try:
                # You might need to add a method to get active clients from gRPC server
                # For now, return empty dict if not available
                return {}
            except:
                return {}
    
    active_client_manager = ActiveClientManager(stub)
    hdfs = HDFSFeatures(active_client_manager=active_client_manager)
    block_manager = BlockManager(active_client_manager=active_client_manager, hdfs_features=hdfs)
    print("Connected to gRPC server for active client management")
    
except Exception as e:
    # Fallback to basic HDFSFeatures if gRPC not available
    hdfs = HDFSFeatures()
    block_manager = BlockManager(hdfs_features=hdfs)
    print(f"gRPC connection failed: {e}. Using basic HDFSFeatures.")

@app.route('/')
def index():
    return '''
    <form action="/upload" method="post" enctype="multipart/form-data">
        <input type="file" name="file">
        <input type="submit" value="Upload">
    </form>
    <form action="/download" method="get">
        <select name="filename">
            {}
        </select>
        <input type="submit" value="Download">
    </form>
    '''.format(''.join(['<option value="{}">{}</option>'.format(f, f) for f in os.listdir(UPLOAD_FOLDER)]))

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return 'No file part'
    file = request.files['file']
    if file.filename == '':
        return 'No selected file'
    
    filename = secure_filename(file.filename) if file.filename else "unknown_file"
    for i in range(1000000):
        pass
    
    # Generate unique filename
    unique_filename = str(uuid.uuid4()) + '_' + filename
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
    file.save(filepath)
    
    # Use HDFS features for advanced file management
    try:
        # Step 1: Split file into blocks (without assigning to nodes)
        print(f"🔄 Splitting file {filename} into blocks...")
        blocks = hdfs.split_file_into_blocks(filepath)
        
        # Step 2: Try to assign blocks to available nodes
        print(f"🔄 Attempting to assign {len(blocks)} blocks to nodes...")
        block_manager.assign_pending_blocks()
        
        # Step 3: Store file metadata
        hdfs.store_file_metadata(filename, filepath, blocks)
        
        # Step 4: Get assignment status
        assignment_status = block_manager.get_assignment_status()
        
        return {
            'message': f'File uploaded successfully using port 5000',
            'blocks_created': len(blocks),
            'blocks_assigned': len(blocks) - len(assignment_status.get('unassigned_blocks', [])),
            'blocks_pending': len(assignment_status.get('unassigned_blocks', [])),
            'free_nodes': assignment_status.get('free_nodes', [])
        }
    except Exception as e:
        return f'File uploaded successfully using port 5000 (HDFS features failed: {str(e)})'

@app.route('/assign_blocks')
def assign_pending_blocks():
    """Manually trigger block assignment for pending blocks"""
    try:
        print("🔄 Manual block assignment triggered...")
        block_manager.assign_pending_blocks()
        
        status = block_manager.get_assignment_status()
        return {
            'message': 'Block assignment completed',
            'status': status
        }
    except Exception as e:
        return {'error': str(e)}

@app.route('/block_status')
def get_block_status():
    """Get current block assignment status"""
    try:
        status = block_manager.get_assignment_status()
        return status
    except Exception as e:
        return {'error': str(e)}

@app.route('/download')
def download_file():
    filename = request.args.get('filename')
    if filename:
        print(f"file: {filename} is downloaded from port 5000")
        return send_file(os.path.join(app.config['UPLOAD_FOLDER'], filename), as_attachment=True)
    else:
        return 'No file selected for download'

@app.route('/download_complete')
def download_complete():
    return 'File downloaded using port 5000'

@app.route('/stats')
def system_stats():
    """Get HDFS system statistics"""
    try:
        stats = hdfs.get_system_stats()
        assignment_status = block_manager.get_assignment_status()
        
        return {
            'server': 'Server 1 (Port 5000)',
            'stats': stats,
            'files': hdfs.list_files(),
            'block_assignment': assignment_status
        }
    except Exception as e:
        return {'error': str(e)}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
