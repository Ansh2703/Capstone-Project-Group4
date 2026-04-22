import os
import yaml


def get_config(env="dev"):
    """
    Load the environment config YAML.
    Resolves the path relative to this file's location (works from any working directory).
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, "..", "..", "config", f"{env}_config.yaml")
    path = os.path.normpath(path)
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_full_table_name(layer, table_name, env="dev"):
    """
    Build a fully qualified table name from config.
    Example: get_full_table_name("bronze", "transactions") -> "maven_market_uc.bronze.transactions"
    """
    config = get_config(env)
    catalog = config["catalog"]
    schema = config["schemas"][layer]
    return f"{catalog}.{schema}.{table_name}"
