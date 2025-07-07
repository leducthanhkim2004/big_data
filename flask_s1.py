from flask import Flask, request, send_file, redirect, url_for
import os
from werkzeug.utils import secure_filename
import uuid
from hdfs_features import HDFSFeatures

UPLOAD_FOLDER = 'uploads'
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

hdfs = HDFSFeatures()

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
        # Split file into blocks
        blocks = hdfs.split_file_into_blocks(filepath)
        
        # Replicate blocks
        replication_map = hdfs.replicate_blocks(blocks)
        
        # Store metadata
        hdfs.store_file_metadata(filename, filepath, blocks)
        
        return f'File uploaded successfully using port 5000 with HDFS features. Blocks: {len(blocks)}, Replicas: {len(replication_map)}'
    except Exception as e:
        return f'File uploaded successfully using port 5000 (HDFS features failed: {str(e)})'

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
        return {
            'server': 'Server 1 (Port 5000)',
            'stats': stats,
            'files': hdfs.list_files()
        }
    except Exception as e:
        return {'error': str(e)}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
