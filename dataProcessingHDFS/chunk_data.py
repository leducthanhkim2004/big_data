import os
import csv
from hdfs import InsecureClient

# Define the path to your local file and the HDFS directory
file_path = r"C:\Users\leduc\big_data\cdets.csv"  # Path to the local CSV file
hdfs_directory = "/user/hadoop/data/chunks"  # HDFS directory to upload the chunks
lines_per_chunk = 500  # Number of lines per chunk (you can adjust this)
hdfs_url = 'http://localhost:9870'  # HDFS NameNode URL
user = 'hdfs'  # HDFS user

def chunk_and_write_to_hdfs(local_file_path, hdfs_directory, lines_per_chunk=500, hdfs_url='http://localhost:9870', user='hdfs'):
    # Initialize HDFS client
    client = InsecureClient(hdfs_url, user=user)
    
    # Open the local CSV file and read the header
    with open(local_file_path, 'r', encoding='utf-8') as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)  # Read the header row
        
        # Initialize variables
        chunk = []
        chunk_idx = 0

        # Iterate over each row in the CSV file
        for row in csv_reader:
            chunk.append(row)  # Add row to the chunk
            if len(chunk) == lines_per_chunk:
                # When the chunk reaches the specified number of rows, write to HDFS
                chunk_filename = f"chunk_{chunk_idx}.csv"
                hdfs_chunk_path = os.path.join(hdfs_directory, chunk_filename)
                
                # Write the chunk to HDFS, include the header row in each chunk
                with client.write(hdfs_chunk_path, encoding='utf-8') as writer:
                    csv_writer = csv.writer(writer)
                    csv_writer.writerow(header)  # Write the header in each chunk
                    csv_writer.writerows(chunk)  # Write the rows in the chunk
                
                print(f"Wrote {hdfs_chunk_path}")
                chunk = []  # Reset the chunk for the next set of rows
                chunk_idx += 1
        
        # Write any remaining rows as the last chunk
        if chunk:
            chunk_filename = f"chunk_{chunk_idx}.csv"
            hdfs_chunk_path = os.path.join(hdfs_directory, chunk_filename)
            with client.write(hdfs_chunk_path, encoding='utf-8') as writer:
                csv_writer = csv.writer(writer)
                csv_writer.writerow(header)  # Write the header in the last chunk
                csv_writer.writerows(chunk)  # Write the remaining rows
            print(f"Wrote {hdfs_chunk_path}")

# Call the function to chunk the data and upload to HDFS
chunk_and_write_to_hdfs(file_path, hdfs_directory, lines_per_chunk, hdfs_url, user)
