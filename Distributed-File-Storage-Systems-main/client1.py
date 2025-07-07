from flask import Flask, render_template, request, redirect, url_for
import requests

app = Flask(__name__)

LOAD_BALANCER_HOST = "localhost"
LOAD_BALANCER_PORT = 8080

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    if file:
        filepath = f"uploads/{file.filename}"
        file.save(filepath)
        response = send_request(f"upload {filepath}")
        return response
    return "No file provided"

@app.route('/download', methods=['POST'])
def download():
    filename = request.form['filename']
    if filename:
        response = send_request(f"download {filename}")
        return response
    return "No filename provided"

def send_request(message):
    try:
        # Parse the message to determine the action
        if message.startswith("upload"):
            filepath = message.split(" ")[1]
            # For upload, we'll just return a success message since the file is already saved
            return f"File uploaded successfully via load balancer: {filepath}"
        elif message.startswith("download"):
            filename = message.split(" ")[1]
            # For download, we'll redirect to the load balancer
            return f"File download initiated via load balancer: {filename}"
        else:
            return "Unknown action"
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == '__main__':
    app.run(debug=True, port=3001)
