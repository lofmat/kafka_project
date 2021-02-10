import yaml
import logging


# Load data from yaml file
def read_yaml(file):
    with open(file) as stream:
        yaml_data = yaml.load(stream, Loader=yaml.FullLoader)
    return yaml_data


# def setup_logger(log_path):
#     logging.basicConfig()
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
#         handlers=[logging.FileHandler(log_path), logging.StreamHandler()]
#     )
