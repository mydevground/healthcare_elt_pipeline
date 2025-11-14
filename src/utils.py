import logging
import yaml

def setup_logging(log_path, level="INFO"):
    logging.basicConfig(filename=log_path, level=getattr(logging, level),
                        format='%(asctime)s - %(levelname)s - %(message)s')
