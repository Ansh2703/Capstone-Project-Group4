import os
import yaml


def get_config(env="dev"):
    """
    Load the environment config YAML.
    Resolves the path relative to this file's location (works from any working directory).

    After loading, any value in the 'paths' or 'checkpoints' sections that does
    NOT start with 'abfss://' is treated as a relative suffix and automatically
    prepended with the top-level 'storage_root' value.  This keeps the YAML DRY
    while remaining backward-compatible with fully-qualified ADLS paths.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, "..", "..", "config", f"{env}_config.yaml")
    path = os.path.normpath(path)
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    # Auto-resolve relative paths using storage_root
    storage_root = config.get("storage_root", "").rstrip("/")
    for section in ("paths", "checkpoints"):
        if section in config and isinstance(config[section], dict):
            for key, val in config[section].items():
                if isinstance(val, str) and not val.startswith("abfss://"):
                    config[section][key] = f"{storage_root}/{val.lstrip('/')}"

    return config


def get_full_table_name(layer, table_name, env="dev"):
    """
    Build a fully qualified table name from config.
    Example: get_full_table_name("bronze", "transactions") -> "maven_market_uc.bronze.transactions"
    """
    config = get_config(env)
    catalog = config["catalog"]
    schema = config["schemas"][layer]
    return f"{catalog}.{schema}.{table_name}"
