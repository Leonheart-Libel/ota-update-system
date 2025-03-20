import os
import sys
import json
import time
import random
import logging
import datetime
import threading
import socket
import psutil
from flask import Flask, jsonify

# Add at the beginning of app.py
try:
    import pyodbc
    HAS_PYODBC = True
except ImportError:
    logger.warning("pyodbc not available, database functionality will be limited")
    HAS_PYODBC = False

# Then modify the DataGenerator class:
def send_to_database(self, data):
    if not self.connection_string:
        logger.warning("No database connection string provided")
        return False
        
    if not HAS_PYODBC:
        logger.warning("pyodbc not available, storing data locally instead")
        return self.store_locally(data)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("IoT_App")

# Load version info
try:
    with open('version.json', 'r') as f:
        version_info = json.load(f)
    VERSION = version_info.get('version', 'unknown')
except Exception as e:
    logger.error(f"Failed to load version info: {e}")
    VERSION = "unknown"

# Load configuration
try:
    # Try relative path first
    config_path = '../ota_mechanism/config.json'
    if not os.path.exists(config_path):
        # Try absolute path
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../ota_mechanism/config.json')
    
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
        AZURE_DB_CONNECTION_STRING = config.get('azure_db_connection_string')
    else:
        logger.error("Config file not found")
        AZURE_DB_CONNECTION_STRING = None
except Exception as e:
    logger.error(f"Failed to load config: {e}")
    AZURE_DB_CONNECTION_STRING = None

# Initialize Flask app for health checks
app = Flask(__name__)

class DataGenerator:
    def __init__(self, connection_string=None):
        """Initialize the data generator."""
        self.connection_string = connection_string
        self.running = False
        self.thread = None
        self.device_id = socket.gethostname()
        self.data_points_generated = 0
        self.data_points_sent = 0
        self.start_time = None
        self.last_error = None
        
    def generate_sensor_data(self):
        """Generate random sensor data."""
        return {
            "device_id": self.device_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "temperature": round(random.uniform(20.0, 30.0), 2),
            "humidity": round(random.uniform(30.0, 70.0), 2),
            "pressure": round(random.uniform(990.0, 1010.0), 2),
            "battery": round(random.uniform(3.0, 4.2), 2),
            "version": VERSION
        }
    
    def send_to_database(self, data):
        """Send data to Azure SQL Database."""
        if not self.connection_string:
            logger.warning("No database connection string provided")
            return False
            
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Create table if it doesn't exist
            cursor.execute('''
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sensor_data')
                BEGIN
                    CREATE TABLE sensor_data (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        device_id NVARCHAR(50),
                        timestamp DATETIME2,
                        temperature FLOAT,
                        humidity FLOAT,
                        pressure FLOAT,
                        battery FLOAT,
                        version NVARCHAR(20)
                    )
                END
            ''')
            
            # Insert data
            cursor.execute('''
                INSERT INTO sensor_data (device_id, timestamp, temperature, humidity, pressure, battery, version)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                data["device_id"],
                data["timestamp"],
                data["temperature"],
                data["humidity"],
                data["pressure"],
                data["battery"],
                data["version"]
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.data_points_sent += 1
            return True
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Failed to send data to database: {e}")
            return False
    
    def store_locally(self, data):
        """Store data locally if database connection fails."""
        try:
            os.makedirs("local_data", exist_ok=True)
            
            # Store in a timestamped file
            filename = f"local_data/data_{datetime.datetime.now().strftime('%Y%m%d')}.jsonl"
            
            with open(filename, 'a') as f:
                f.write(json.dumps(data) + '\n')
                
            return True
        except Exception as e:
            logger.error(f"Failed to store data locally: {e}")
            return False
            
    def data_generation_loop(self):
        """Main data generation loop."""
        self.start_time = datetime.datetime.now()
        
        while self.running:
            try:
                # Generate sensor data
                data = self.generate_sensor_data()
                self.data_points_generated += 1
                
                # Try to send to database
                sent = self.send_to_database(data)
                
                # If failed, store locally
                if not sent:
                    self.store_locally(data)
                
                # Log progress every 100 data points
                if self.data_points_generated % 100 == 0:
                    elapsed = (datetime.datetime.now() - self.start_time).total_seconds()
                    rate = self.data_points_generated / elapsed if elapsed > 0 else 0
                    logger.info(f"Generated {self.data_points_generated} data points, sent {self.data_points_sent} to DB ({rate:.2f} points/sec)")
                
                # Sleep between data points
                time.sleep(5)  # Generate data every 5 seconds
            except Exception as e:
                logger.error(f"Error in data generation loop: {e}")
                time.sleep(10)  # Longer sleep on error
    
    def start(self):
        """Start the data generation thread."""
        if self.running:
            logger.warning("Data generator already running")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self.data_generation_loop)
        self.thread.daemon = True
        self.thread.start()
        logger.info("Data generator started")
    
    def stop(self):
        """Stop the data generation thread."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=10)
        logger.info("Data generator stopped")
    
    def get_stats(self):
        """Get current statistics."""
        uptime = (datetime.datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            "device_id": self.device_id,
            "version": VERSION,
            "uptime_seconds": uptime,
            "data_points_generated": self.data_points_generated,
            "data_points_sent": self.data_points_sent,
            "generation_rate": self.data_points_generated / uptime if uptime > 0 else 0,
            "success_rate": (self.data_points_sent / self.data_points_generated * 100) if self.data_points_generated > 0 else 0,
            "last_error": self.last_error
        }

# Create data generator instance
data_generator = DataGenerator(connection_string=AZURE_DB_CONNECTION_STRING)

@app.route('/health')
def health_check():
    """Health check endpoint for the OTA updater."""
    # Get system info
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    # Get application stats
    app_stats = data_generator.get_stats()
    
    health_data = {
        "status": "healthy",
        "version": VERSION,
        "timestamp": datetime.datetime.now().isoformat(),
        "system": {
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": memory.percent,
            "memory_available_mb": memory.available / (1024 * 1024),
            "disk_percent": disk.percent,
            "disk_free_gb": disk.free / (1024 * 1024 * 1024)
        },
        "application": app_stats
    }
    
    # Determine if the application is healthy
    is_healthy = (
        app_stats["uptime_seconds"] > 0 and
        (app_stats["data_points_generated"] > 0 or app_stats["uptime_seconds"] < 60)  # Allow up to 60 seconds to generate first data point
    )
    
    if not is_healthy:
        health_data["status"] = "unhealthy"
    
    return jsonify(health_data)

@app.route('/')
def index():
    """Main application endpoint."""
    stats = data_generator.get_stats()
    return jsonify({
        "status": "running",
        "app_name": "IoT Device Simulator",
        "version": VERSION,
        "stats": stats
    })

def start_web_server():
    """Start the Flask web server in a separate thread."""
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)).start()
    logger.info("Web server started on port 8080")

if __name__ == "__main__":
    try:
        # Start the web server
        start_web_server()
        
        # Start data generation
        data_generator.start()
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Application stopping due to keyboard interrupt")
        data_generator.stop()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        data_generator.stop()

# Then modify your DataGenerator class to handle this case
def send_to_database(self, data):
    """Send data to Azure SQL Database."""
    if not self.connection_string:
        logger.warning("No database connection string provided")
        return False
        
    if not HAS_PYODBC:
        logger.warning("pyodbc not available, storing data locally instead")
        return self.store_locally(data)
        
    try:
        conn = pyodbc.connect(self.connection_string)
        # Rest of your code...
    except Exception as e:
        self.last_error = str(e)
        logger.error(f"Failed to send data to database: {e}")
        return False