import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import yadisk

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  
formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logging.getLogger("yadisk").setLevel(logging.WARNING)

def get_config():
    return {
        'app_id': os.getenv('APP_ID'),
        'secret_id': os.getenv('SECRET_ID'),
        'ya_token': os.getenv('YA_TOKEN'),
        'local_path': os.getenv('LOCAL_PATH'),
        'hash_path': os.getenv('HASH_PATH'),
        'username': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database_name': os.getenv('DB_NAME'),
        'host': os.getenv('DB_HOST'),
    }
    
def init_services():
    config = get_config()
    yadisk_client = yadisk.YaDisk(
        config['app_id'], 
        config['secret_id'], 
        config['ya_token']
    )

    engine = create_engine(
        f'postgresql://{config["username"]}:{config["password"]}@{config["host"]}/{config["database_name"]}'
    )
    
    return config, yadisk_client, engine

config, yadisk_client, engine = init_services()



