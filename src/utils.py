import yaml


# Load data from yaml file
def read_yaml(file):
    with open(file) as stream:
        yaml_data = yaml.load(stream)
    return yaml_data