import os
import sys
import json
import time
import logging
import hashlib
import shutil
import requests
import subprocess
import threading
from datetime import datetime
from typing import Dict, Any, Optional
from azure_db_helper import AzureDBHelper


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ota_updater.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("OTA_Updater")

class OTAUpdater:
    def __init__(self, config_path: str = "config.json"):
        """Initialize the OTA updater with configuration."""
        self.config = self._load_config(config_path)
        self.github_token = self.config.get("github_token")
        self.repo_owner = self.config.get("repo_owner")
        self.repo_name = self.config.get("repo_name")
        self.app_path = self.config.get("app_path")
        self.backup_path = self.config.get("backup_path")
        self.update_interval = self.config.get("update_interval", 3600)  # Default: 1 hour
        self.app_process = None
        self.health_monitor = None
        self.last_healthy_version = None
        self.initialize_paths()
        self.initialize_db_connection()
    def initialize_db_connection(self):
        """Initialize the Azure database connection."""
        try:
            self.db_helper = AzureDBHelper(self.config.get("azure_db_connection_string"))
            if not self.db_helper.test_connection():
                logger.warning("Failed to connect to Azure database")
            else:
                logger.info("Successfully connected to Azure database")
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            self.db_helper = None
        
    def initialize_paths(self) -> None:
        """Ensure all necessary directories exist."""
        os.makedirs(self.app_path, exist_ok=True)
        os.makedirs(self.backup_path, exist_ok=True)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            # Default configuration
            return {
                "github_token": "",
                "repo_owner": "",
                "repo_name": "",
                "app_path": "./application",
                "backup_path": "./backup",
                "update_interval": 3600,
                "health_check_interval": 60,
                "health_check_timeout": 10
            }
    
    def _get_current_version(self) -> Optional[Dict[str, Any]]:
        """Get current application version from version.json."""
        try:
            version_path = os.path.join(self.app_path, "version.json")
            if os.path.exists(version_path):
                with open(version_path, 'r') as f:
                    return json.load(f)
            return None
        except Exception as e:
            logger.error(f"Failed to get current version: {e}")
            return None
    
    def _get_latest_version(self) -> Optional[Dict[str, Any]]:
        """Fetch the latest version information from GitHub."""
        try:
            url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/contents/application/version.json"
            headers = {
                "Accept": "application/vnd.github.v3.raw",
                "Authorization": f"token {self.github_token}"
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch latest version: {e}")
            return None
    
    def _download_file(self, github_path: str, local_path: str) -> bool:
        """Download a file from GitHub repository."""
        try:
            url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/contents/{github_path}"
            headers = {
                "Accept": "application/vnd.github.v3.raw",
                "Authorization": f"token {self.github_token}"
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'wb') as f:
                f.write(response.content)
            return True
        except Exception as e:
            logger.error(f"Failed to download {github_path}: {e}")
            return False
    
    def _list_directory_contents(self, github_path: str) -> list:
        """List contents of a directory in GitHub repository."""
        try:
            url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/contents/{github_path}"
            headers = {
                "Authorization": f"token {self.github_token}"
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to list directory contents for {github_path}: {e}")
            return []
    
    def backup_current_application(self) -> bool:
        """Create a backup of the current application."""
        try:
            if not os.path.exists(self.app_path):
                logger.warning("No application to backup")
                return False
                
            # Create timestamp for backup folder
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            current_version = self._get_current_version()
            version_str = current_version.get("version", "unknown") if current_version else "unknown"
            backup_dir = os.path.join(self.backup_path, f"{version_str}_{timestamp}")
            
            # Copy all files from app_path to backup_dir
            shutil.copytree(self.app_path, backup_dir)
            logger.info(f"Application backed up to {backup_dir}")
            return True
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False
    
    def restore_from_backup(self, backup_dir: Optional[str] = None) -> bool:
        """Restore application from a backup."""
        try:
            # If no specific backup provided, use the most recent one
            if not backup_dir:
                backups = [os.path.join(self.backup_path, d) for d in os.listdir(self.backup_path)]
                if not backups:
                    logger.error("No backups found")
                    return False
                backup_dir = max(backups, key=os.path.getctime)
            
            # Remove current application
            if os.path.exists(self.app_path):
                shutil.rmtree(self.app_path)
            
            # Copy backup to application directory
            shutil.copytree(backup_dir, self.app_path)
            logger.info(f"Application restored from {backup_dir}")
            return True
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False
    
    def download_update(self) -> bool:
        """Download the latest application version from GitHub."""
        try:
            # Get the list of files in the application directory
            contents = self._list_directory_contents("application")
            if not contents:
                return False
                
            # Create temporary directory for downloading
            temp_dir = os.path.join(self.backup_path, "temp_download")
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir)
            
            # Download each file
            for item in contents:
                if item["type"] == "file":
                    github_path = item["path"]
                    local_path = os.path.join(temp_dir, os.path.basename(github_path))
                    if not self._download_file(github_path, local_path):
                        logger.error(f"Failed to download {github_path}")
                        return False
                elif item["type"] == "dir":
                    # Handle subdirectories (recursive list and download)
                    self._download_directory(item["path"], os.path.join(temp_dir, os.path.basename(item["path"])))
            
            # Move downloaded files to application directory
            if os.path.exists(self.app_path):
                shutil.rmtree(self.app_path)
            shutil.move(temp_dir, self.app_path)
            
            logger.info("Update downloaded successfully")
            return True
        except Exception as e:
            logger.error(f"Update download failed: {e}")
            return False
    
    def _download_directory(self, github_path: str, local_dir: str) -> bool:
        """Recursively download a directory from GitHub."""
        try:
            os.makedirs(local_dir, exist_ok=True)
            contents = self._list_directory_contents(github_path)
            
            for item in contents:
                if item["type"] == "file":
                    local_path = os.path.join(local_dir, os.path.basename(item["path"]))
                    if not self._download_file(item["path"], local_path):
                        return False
                elif item["type"] == "dir":
                    self._download_directory(item["path"], os.path.join(local_dir, os.path.basename(item["path"])))
            return True
        except Exception as e:
            logger.error(f"Failed to download directory {github_path}: {e}")
            return False
    
    def check_for_updates(self) -> bool:
        """Check if there are updates available."""
        current_version = self._get_current_version()
        latest_version = self._get_latest_version()
        
        if not latest_version:
            logger.error("Failed to get latest version information")
            return False
            
        if not current_version:
            logger.info("No current version found, will download the latest")
            return True
            
        # Compare versions (semantic versioning)
        current_v = current_version.get("version", "0.0.0")
        latest_v = latest_version.get("version", "0.0.0")
        
        logger.info(f"Current version: {current_v}, Latest version: {latest_v}")
        
        # Simple comparison - in a production environment, use a proper semver library
        current_parts = [int(x) for x in current_v.split('.')]
        latest_parts = [int(x) for x in latest_v.split('.')]
        
        for i in range(max(len(current_parts), len(latest_parts))):
            curr = current_parts[i] if i < len(current_parts) else 0
            latest = latest_parts[i] if i < len(latest_parts) else 0
            
            if latest > curr:
                return True
            elif curr > latest:
                return False
        
        return False  # Versions are equal
    
    def perform_health_check(self) -> bool:
        """Check if the application is running correctly."""
        try:
            health_check_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "healthcheck.py")
            result = subprocess.run([sys.executable, health_check_script], 
                                   capture_output=True, 
                                   timeout=self.config.get("health_check_timeout", 10))
            if result.returncode == 0:
                logger.info("Health check passed")
                # Update last known healthy version
                current_version = self._get_current_version()
                if current_version and current_version.get("version"):
                    self.last_healthy_version = current_version.get("version")
                return True
            else:
                logger.error(f"Health check failed: {result.stdout.decode()} {result.stderr.decode()}")
                return False
        except subprocess.TimeoutExpired:
            logger.error("Health check timed out")
            return False
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return False
    
    def start_application(self) -> bool:
        """Start the application in a subprocess."""
        try:
            app_script = os.path.join(self.app_path, "app.py")
            if not os.path.exists(app_script):
                logger.error(f"Application script not found: {app_script}")
                return False
                
            # Kill any existing process
            self.stop_application()
            
            # Install dependencies with better error handling
            req_file = os.path.join(self.app_path, "requirements.txt")
            if os.path.exists(req_file):
                try:
                    # Try to install with pip
                    logger.info("Installing dependencies...")
                    # Add --no-build-isolation flag to use system packages
                    result = subprocess.run(
                        [sys.executable, "-m", "pip", "install", "-r", req_file, "--no-build-isolation"],
                        capture_output=True,
                        text=True,
                        check=False  # Don't raise exception on non-zero exit
                    )
                    
                    if result.returncode != 0:
                        logger.error(f"Failed to install dependencies: {result.stderr}")
                        # Try to install packages one by one
                        with open(req_file, 'r') as f:
                            packages = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                        
                        logger.info("Attempting to install packages individually...")
                        for package in packages:
                            try:
                                subprocess.run(
                                    [sys.executable, "-m", "pip", "install", package, "--no-build-isolation"],
                                    check=True
                                )
                                logger.info(f"Successfully installed {package}")
                            except Exception as e:
                                logger.error(f"Failed to install {package}: {e}")
                    else:
                        logger.info("Dependencies installed successfully")
                except Exception as e:
                    logger.error(f"Failed to install dependencies: {e}")
                
            # Start application
            self.app_process = subprocess.Popen([sys.executable, app_script])
            logger.info(f"Application started with PID {self.app_process.pid}")
            return True
        except Exception as e:
            logger.error(f"Failed to start application: {e}")
            return False
    
    def stop_application(self) -> None:
        """Stop the running application."""
        if self.app_process:
            try:
                self.app_process.terminate()
                self.app_process.wait(timeout=5)
                logger.info("Application stopped")
            except subprocess.TimeoutExpired:
                self.app_process.kill()
                logger.warning("Application killed after timeout")
            except Exception as e:
                logger.error(f"Error stopping application: {e}")
            finally:
                self.app_process = None
    
    def update_if_available(self) -> bool:
        """Check for and apply updates if available."""
        if not self.check_for_updates():
            logger.info("No updates available")
            return False
                
        logger.info("Update available, starting update process")
        
        # Get current and latest version info
        current_version = self._get_current_version()
        latest_version = self._get_latest_version()
        current_v = current_version.get("version", "0.0.0") if current_version else "0.0.0"
        latest_v = latest_version.get("version", "0.0.0") if latest_version else "0.0.0"
        
        # Log update start
        self.log_update_to_db(latest_v, "started", {
            "previous_version": current_v,
            "reason": "scheduled_check"
        })
        
        # Backup current application
        if not self.backup_current_application():
            logger.error("Failed to backup current application, aborting update")
            self.log_update_to_db(latest_v, "failed", {
                "stage": "backup",
                "error": "Failed to backup current application"
            })
            return False
                
        # Stop application
        self.stop_application()
        
        # Download and apply update
        if not self.download_update():
            logger.error("Failed to download update, rolling back")
            self.restore_from_backup()
            self.start_application()
            self.log_update_to_db(latest_v, "failed", {
                "stage": "download",
                "error": "Failed to download update",
                "action": "rolled_back"
            })
            return False
                
        # Start updated application
        if not self.start_application():
            logger.error("Failed to start updated application, rolling back")
            self.restore_from_backup()
            self.start_application()
            self.log_update_to_db(latest_v, "failed", {
                "stage": "startup",
                "error": "Failed to start updated application",
                "action": "rolled_back"
            })
            return False
                
        # Perform health check
        if not self.perform_health_check():
            logger.error("Health check failed after update, rolling back")
            self.stop_application()
            self.restore_from_backup()
            self.start_application()
            self.log_update_to_db(latest_v, "failed", {
                "stage": "health_check",
                "error": "Health check failed",
                "action": "rolled_back"
            })
            return False
                
        logger.info("Update successful")
        
        # Log update success
        self.log_update_to_db(latest_v, "completed", {
            "previous_version": current_v,
            "health_check": "passed"
        })
        
        return True
    
    def run_continuous_updates(self) -> None:
        """Run the update check process continuously."""
        try:
            # Initial application start
            if not os.path.exists(os.path.join(self.app_path, "app.py")):
                logger.info("Initial application download")
                self.download_update()
            
            self.start_application()
            
            # Continuous health check and update loop
            while True:
                try:
                    # Perform health check
                    if not self.perform_health_check():
                        logger.warning("Health check failed, attempting to restart application")
                        self.stop_application()
                        self.start_application()
                        
                        # If it fails again, try to rollback
                        if not self.perform_health_check():
                            logger.error("Health check failed after restart, rolling back")
                            self.restore_from_backup()
                            self.start_application()
                    
                    # Check for updates
                    self.update_if_available()
                    
                except Exception as e:
                    logger.error(f"Error in update loop: {e}")
                
                # Wait for next check
                time.sleep(self.update_interval)
        except KeyboardInterrupt:
            logger.info("Update process terminated by user")
            self.stop_application()
        except Exception as e:
            logger.error(f"Unexpected error in continuous update process: {e}")
            self.stop_application()

    def log_update_to_db(self, version, status, details=None):
        """Log update information to Azure database."""
        if not hasattr(self, "db_helper") or self.db_helper is None:
            logger.warning("Database helper not initialized, can't log update")
            return
            
        try:
            # Create log entry
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "version": version,
                "status": status,
                "details": details or {}
            }
            
            # Convert to JSON
            log_data = json.dumps(log_entry).encode('utf-8')
            
            # Upload to blob storage
            blob_name = f"update_logs/{version}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
            self.db_helper.upload_to_blob("ota-logs", blob_name, log_data)
            
            logger.info(f"Update log saved to Azure: {blob_name}")
        except Exception as e:
            logger.error(f"Failed to log update to database: {e}")

if __name__ == "__main__":
    updater = OTAUpdater()
    updater.run_continuous_updates()