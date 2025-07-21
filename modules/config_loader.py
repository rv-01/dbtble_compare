import yaml
import os

def load_config(config_path):
    """
    Loads and validates the YAML configuration file.
    Returns the config as a dictionary.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    # Basic validation
    required_sections = ['source_db', 'target_db', 'table_config', 'paths', 'flags']
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required section in config: {section}")
    if not isinstance(config['table_config'], list):
        raise ValueError("table_config must be a list of table definitions.")
    return config 