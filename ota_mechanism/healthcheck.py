import os
import sys
import json
import time
import logging
import requests
import subprocess
import socket
import psutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("healthcheck.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Health_Check")

def load_config(config_path="config.json"):
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {
            "app_path": "./application",
            "health_check_port": 8080,
            "health_check_endpoint": "/health",
            "min_memory_mb": 50,
            "max_cpu_percent": 90
        }

def check_app_running():
    """Check if the application process is running."""
    try:
        config = load_config()
        app_script = os.path.join(config.get("app_path"), "app.py")
        
        # Check for python processes running app.py
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            cmdline = proc.info.get('cmdline', [])
            if any(app_script in cmd for cmd in cmdline if cmd):
                logger.info(f"Application process found with PID {proc.info['pid']}")
                return True
        
        logger.error("Application process not found")
        return False
    except Exception as e:
        logger.error(f"Error checking if app is running: {e}")
        return False

def check_endpoint_health():
    """Check if the application's health endpoint is responding."""
    try:
        config = load_config()
        port = config.get("health_check_port", 8080)
        endpoint = config.get("health_check_endpoint", "/health")
        
        url = f"http://localhost:{port}{endpoint}"
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            logger.info("Health endpoint check passed")
            health_data = response.json()
            
            # Check if response contains expected fields
            if health_data.get("status") == "healthy":
                return True
            else:
                logger.error(f"Health endpoint returned unhealthy status: {health_data}")
                return False
        else:
            logger.error(f"Health endpoint returned status code {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to connect to health endpoint: {e}")
        return False
    except Exception as e:
        logger.error(f"Error checking endpoint health: {e}")
        return False

def check_resource_usage():
    """Check if the application is using reasonable resources."""
    try:
        config = load_config()
        min_memory_mb = config.get("min_memory_mb", 50)
        max_cpu_percent = config.get("max_cpu_percent", 90)
        app_script = os.path.join(config.get("app_path"), "app.py")
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_info', 'cpu_percent']):
            cmdline = proc.info.get('cmdline', [])
            if any(app_script in cmd for cmd in cmdline if cmd):
                # Update CPU usage
                proc.cpu_percent()
                time.sleep(0.5)  # Wait a moment for accurate CPU measurement
                cpu_percent = proc.cpu_percent()
                
                memory_mb = proc.memory_info().rss / (1024 * 1024)
                
                logger.info(f"App using {memory_mb:.2f} MB memory, {cpu_percent:.2f}% CPU")
                
                # Check if memory is too low (might indicate a problem)
                if memory_mb < min_memory_mb:
                    logger.warning(f"App memory usage too low: {memory_mb:.2f} MB")
                    return False
                
                # Check if CPU usage is too high
                if cpu_percent > max_cpu_percent:
                    logger.warning(f"App CPU usage too high: {cpu_percent:.2f}%")
                    return False
                
                return True
        
        logger.error("Could not find application process for resource check")
        return False
    except Exception as e:
        logger.error(f"Error checking resource usage: {e}")
        return False

def check_data_generation():
    """Check if the application is generating data (by checking log file updates)."""
    try:
        config = load_config()
        app_path = config.get("app_path")
        log_file = os.path.join(app_path, "app.log")
        
        if not os.path.exists(log_file):
            logger.warning(f"Log file not found: {log_file}")
            return True  # Skip this check if log file doesn't exist
        
        # Check if log file has been updated recently
        mod_time = os.path.getmtime(log_file)
        current_time = time.time()
        
        # If log file was modified within the last minute, it's probably active
        if current_time - mod_time < 60:
            logger.info("Log file recently updated, app appears to be generating data")
            return True
        else:
            logger.warning(f"Log file not updated recently (last update: {time.ctime(mod_time)})")
            return False
    except Exception as e:
        logger.error(f"Error checking data generation: {e}")
        return True  # Don't fail health check on this error

def check_version_file():
    """Check if version.json exists and has valid format."""
    try:
        config = load_config()
        app_path = config.get("app_path")
        version_file = os.path.join(app_path, "version.json")
        
        if not os.path.exists(version_file):
            logger.error(f"Version file not found: {version_file}")
            return False
        
        with open(version_file, 'r') as f:
            version_data = json.load(f)
        
        required_fields = ["version", "release_notes"]
        
        for field in required_fields:
            if field not in version_data:
                logger.error(f"Version file missing required field: {field}")
                return False
        
        logger.info(f"Version file check passed: version {version_data.get('version')}")
        return True
    except json.JSONDecodeError:
        logger.error("Version file contains invalid JSON")
        return False
    except Exception as e:
        logger.error(f"Error checking version file: {e}")
        return False

def run_all_checks():
    """Run all health checks and return overall status."""
    checks = {
        "app_running": check_app_running(),
        "endpoint_health": check_endpoint_health(),
        "resource_usage": check_resource_usage(),
        "data_generation": check_data_generation(),
        "version_file": check_version_file()
    }
    
    # Log all check results
    for check_name, result in checks.items():
        logger.info(f"Check '{check_name}': {'PASSED' if result else 'FAILED'}")
    
    # Overall health is ok if all critical checks pass
    critical_checks = ["app_running", "endpoint_health", "version_file"]
    health_status = all(checks[check] for check in critical_checks)
    
    if health_status:
        logger.info("HEALTH CHECK PASSED")
        return 0
    else:
        logger.error("HEALTH CHECK FAILED")
        return 1

# Add to healthcheck.py - add this function

def check_azure_db_connection():
    """Check if the Azure DB connection is working."""
    try:
        config = load_config()
        
        # Skip check if no connection string configured
        if not config.get("azure_db_connection_string"):
            logger.info("No Azure DB connection string configured, skipping check")
            return True
            
        # Import the helper module
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from azure_db_helper import AzureDBHelper
        
        # Initialize and test
        helper = AzureDBHelper(config.get("azure_db_connection_string"))
        if helper.test_connection():
            logger.info("Azure DB connection check passed")
            return True
        else:
            logger.error("Azure DB connection check failed")
            return False
    except Exception as e:
        logger.error(f"Error checking Azure DB connection: {e}")
        return False

# Then update the run_all_checks function to include this check:

def run_all_checks():
    """Run all health checks and return overall status."""
    checks = {
        "app_running": check_app_running(),
        "endpoint_health": check_endpoint_health(),
        "resource_usage": check_resource_usage(),
        "data_generation": check_data_generation(),
        "version_file": check_version_file(),
        "azure_db_connection": check_azure_db_connection()  # Add this line
    }
    
    # Log all check results
    for check_name, result in checks.items():
        logger.info(f"Check '{check_name}': {'PASSED' if result else 'FAILED'}")
    
    # Overall health is ok if all critical checks pass
    critical_checks = ["app_running", "endpoint_health", "version_file"]
    health_status = all(checks[check] for check in critical_checks)
    
    if health_status:
        logger.info("HEALTH CHECK PASSED")
        return 0
    else:
        logger.error("HEALTH CHECK FAILED")
        return 1

if __name__ == "__main__":
    try:
        exit_code = run_all_checks()
        sys.exit(exit_code)
    except Exception as e:
        logger.critical(f"Unhandled exception in health check: {e}")
        sys.exit(1)