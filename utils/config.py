import yaml
from databricks.sdk.runtime import dbutils
from pathlib import Path

def load_config():
    """Load configuration from YAML file"""
    config_path = Path(__file__).parent.parent.parent / "resources" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_adls_config():
    """Get ADLS configuration"""
    config = load_config()
    return {
        "storage_account": config["storage"]["account"],
        "container": config["storage"]["container"],
        "landing_path": config["storage"]["paths"]["landing"],
        "bronze_path": config["storage"]["paths"]["bronze"],
        "silver_path": config["storage"]["paths"]["silver"]
    }

def get_service_principal_credentials():
    """Get service principal credentials from Databricks secrets"""
    return {
        "client_id": dbutils.secrets.get("EcoWindCreds", "client_id"),
        "client_secret": dbutils.secrets.get("EcoWindCreds", "client_secret"),
        "tenant_id": dbutils.secrets.get("EcoWindCreds", "tenant_id")
    }

def get_catalog_config():
    """Get Unity Catalog configuration"""
    config = load_config()
    return {
        "catalog": config["catalog"]["name"],
        "schema": config["catalog"]["schema"],
        "tables": config["catalog"]["tables"]
    } 