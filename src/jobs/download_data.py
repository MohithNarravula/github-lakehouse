import os
import sys
import requests
from dotenv import load_dotenv

# Load environment variables from your .env file
load_dotenv()

def download_gh_data(date_str, hour):
    """
    Downloads GH Archive data for a specific date and hour.
    Example: date_str='2024-01-01', hour='15'
    """
    file_name = f"{date_str}-{hour}.json.gz"
    url = f"https://data.gharchive.org/{file_name}"
    
    # Use the path from your .env
    raw_dir = os.getenv("DATA_RAW_PATH", "/opt/spark/data/raw")
    
    # Ensure the directory exists
    try:
        os.makedirs(raw_dir, exist_ok=True)
        print(f"Directory ready: {raw_dir}")
    except Exception as e:
        print(f"PERMISSION ERROR: Could not create directory {raw_dir}: {e}")
        sys.exit(1)
    
    target_path = os.path.join(raw_dir, file_name)
    
    print(f"Starting download: {url}")
    
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status() 
        
        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        # CRITICAL VERIFICATION: Check if the file size is greater than 0
        if os.path.getsize(target_path) > 0:
            print(f"Success! File saved to: {target_path} ({os.path.getsize(target_path)} bytes)")
        else:
            print(f"Warning: File saved but it is empty: {target_path}")
            sys.exit(1)
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to download data from {url}: {e}")
        sys.exit(1) # Signal failure to Airflow

if __name__ == "__main__":
    # Hard-coded for testing as requested
    date_arg = "2024-01-01"
    hour_arg = "15"
    
    print(f"DEBUG: Starting download job for {date_arg} at hour {hour_arg}")        
    download_gh_data(date_arg, hour_arg)