import os
import json
import logging
from typing import Dict, Any, List, Optional, Tuple

from azure.identity import DefaultAzureCredential
from azure.mgmt.sql import SqlManagementClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("azure_db.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Azure_DB_Helper")

class AzureDBHelper:
    def __init__(self, connection_string: str = None, config_path: str = "config.json"):
        """Initialize the Azure DB Helper with configuration."""
        self.config = self._load_config(config_path)
        
        # Use the provided connection string or get from config
        self.connection_string = connection_string or self.config.get("azure_db_connection_string")
        
        # Parse connection string
        self.connection_params = self._parse_connection_string(self.connection_string)
        
        # Initialize Azure credentials
        self.credential = DefaultAzureCredential()
        
        logger.info("Azure DB Helper initialized")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {}
    
    def _parse_connection_string(self, connection_string: str) -> Dict[str, str]:
        """Parse Azure DB connection string into components."""
        params = {}
        if not connection_string:
            logger.error("No connection string provided")
            return params
            
        try:
            # Handle different connection string formats
            if "AccountKey=" in connection_string:
                # Storage account connection string
                parts = connection_string.split(';')
                for part in parts:
                    if '=' in part:
                        key, value = part.split('=', 1)
                        params[key.strip()] = value.strip()
            else:
                # SQL Database connection string
                parts = connection_string.split(';')
                for part in parts:
                    if '=' in part:
                        key, value = part.split('=', 1)
                        params[key.strip()] = value.strip()
                        
            return params
        except Exception as e:
            logger.error(f"Failed to parse connection string: {e}")
            return {}
    
    def test_connection(self) -> bool:
        """Test the Azure database connection."""
        try:
            # Different approach based on connection type
            if "AccountName" in self.connection_params:
                # Storage account
                account_name = self.connection_params.get("AccountName")
                account_key = self.connection_params.get("AccountKey")
                
                if not account_name or not account_key:
                    logger.error("Missing account name or key")
                    return False
                
                # Create blob service client
                blob_service_client = BlobServiceClient(
                    account_url=f"https://{account_name}.blob.core.windows.net",
                    credential=account_key
                )
                
                # List containers to test connection
                containers = list(blob_service_client.list_containers(max_results=1))
                logger.info(f"Successfully connected to Azure Storage account")
                return True
                
            elif "Server" in self.connection_params:
                # SQL Database
                server = self.connection_params.get("Server")
                database = self.connection_params.get("Database")
                
                if not server or not database:
                    logger.error("Missing server or database name")
                    return False
                
                # Extract subscription and resource group from server name
                # This is a simplified approach - in production, you'd use proper parsing
                # Format: server.database.windows.net
                if ".database.windows.net" in server:
                    server_name = server.split('.')[0]
                    
                    # For testing, we'll just verify the server exists
                    subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")
                    if not subscription_id:
                        logger.error("AZURE_SUBSCRIPTION_ID environment variable not set")
                        return False
                    
                    # Create SQL management client
                    sql_client = SqlManagementClient(self.credential, subscription_id)
                    
                    # List SQL servers to check connection
                    servers = list(sql_client.servers.list())
                    logger.info(f"Successfully connected to Azure SQL management")
                    return True
                    
            logger.error("Unsupported connection string format")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a query against Azure SQL Database.
        
        Note: This is a placeholder. In a real implementation, we would use a pure Python
        SQL client compatible with Python 3.12 (like SQLAlchemy with appropriate driver)
        or stream the query to an Azure Function that handles the database interaction.
        """
        logger.error("Direct query execution not implemented. Use Azure Functions for database operations.")
        return []
    
    def get_table_data(self, table_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get data from a table (placeholder function).
        
        In a real implementation, you would:
        1. Use a Python 3.12 compatible SQL client, or
        2. Call an Azure Function HTTP endpoint, or
        3. Use SDK-specific methods for your use case
        """
        logger.info(f"Placeholder: Would get data from table {table_name}")
        return []
    
    def upload_to_blob(self, container_name: str, blob_name: str, data: bytes) -> bool:
        """Upload data to Azure Blob Storage."""
        try:
            account_name = self.connection_params.get("AccountName")
            account_key = self.connection_params.get("AccountKey")
            
            if not account_name or not account_key:
                logger.error("Missing storage account credentials")
                return False
                
            # Create blob service client
            blob_service_client = BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=account_key
            )
            
            # Create container if it doesn't exist
            try:
                container_client = blob_service_client.get_container_client(container_name)
                container_client.get_container_properties()
            except Exception:
                container_client = blob_service_client.create_container(container_name)
            
            # Upload blob
            blob_client = blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_name
            )
            
            blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Successfully uploaded blob {blob_name} to container {container_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload blob: {e}")
            return False
    
    def download_from_blob(self, container_name: str, blob_name: str) -> Optional[bytes]:
        """Download data from Azure Blob Storage."""
        try:
            account_name = self.connection_params.get("AccountName")
            account_key = self.connection_params.get("AccountKey")
            
            if not account_name or not account_key:
                logger.error("Missing storage account credentials")
                return None
                
            # Create blob service client
            blob_service_client = BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=account_key
            )
            
            # Get blob client
            blob_client = blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_name
            )
            
            # Download blob
            download_stream = blob_client.download_blob()
            data = download_stream.readall()
            
            logger.info(f"Successfully downloaded blob {blob_name} from container {container_name}")
            return data
        except Exception as e:
            logger.error(f"Failed to download blob: {e}")
            return None

# Example usage
if __name__ == "__main__":
    # Load connection string from config
    helper = AzureDBHelper()
    
    # Test connection
    if helper.test_connection():
        print("Connection successful!")
    else:
        print("Connection failed!")