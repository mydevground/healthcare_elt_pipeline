import os
import yaml
from dotenv import load_dotenv

def load_config():
    # Load environment variables from .env
    load_dotenv()

    # Get YAML path from env or fallback to default
    yaml_path = os.getenv("CONFIG_PATH", "config/settings.yaml")

    # Load YAML config
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)

    # Inject secrets from .env
    ### Dummy data - Not used in the project - can be updated for the use case ###
    config["secrets"] = {
        "db_password": os.getenv("DB_PASSWORD"),
        "slack_webhook": os.getenv("SLACK_WEBHOOK"),
        "smtp_user": os.getenv("SMTP_USER"),
        "smtp_pass": os.getenv("SMTP_PASS"),
    }

    return config