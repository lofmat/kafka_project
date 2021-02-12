import yaml
import os


# Load data from yaml file
def read_yaml(file):
    with open(file) as stream:
        yaml_data = yaml.load(stream, Loader=yaml.FullLoader)
    return yaml_data


# Load data from yaml file
def get_yaml_configs():
    config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
    db_schema = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/db_schema.yaml')
    # Read configs
    schema = read_yaml(db_schema)
    params = read_yaml(config)
    return schema, params
