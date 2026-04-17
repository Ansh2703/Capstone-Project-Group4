import yaml

def get_config(env="dev"):
    # This path assumes you are running from a notebook in src/pipelines/
    path = f"../../config/{env}_config.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)

def get_full_table_name(layer, table_name):
    config = get_config()
    catalog = config['catalog']
    schema = config['schemas'][layer]
    return f"{catalog}.{schema}.{table_name}"