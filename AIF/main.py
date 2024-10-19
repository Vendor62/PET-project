import os
import json
import logging
from config import config, yadisk_client, engine
from database import load_to_database
from query import rfm_analysis, cohort_analysis, calculate_cdr, transpon
from utils import download_file, create_datasets, clean_local_files, shutdown

logger = logging.getLogger()

def check_token():
    logger.info('Проверяем токен..')
    try:
        if yadisk_client.check_token():
            logger.info('Токен корректен.')
        else:
            
            logger.warning('Токен некорректен!')
    except Exception as e:
        logger.error(f'Ошибка при проверке токена: {e}')

def extract_and_transform(y, local_path, hash_path, engine):
    """Управляет загрузкой и обработкой."""
    try:
        if os.path.exists(hash_path):
            with open(hash_path, 'r') as f:
                hash_data = json.load(f)
            logging.info("Файл хеша найден. Проверяем изменения на Яндекс.Диске.")
        else:
            logging.info("Файл хеша не найден, скачиваем все файлы.")
            hash_data = {}
            new_data_downloaded = True

        list_of_files = [i['path'] for i in y.listdir('AIF/all_files') if i['path'].endswith('.csv')]
        logger.info("Список файлов на диске:")
        for file in list_of_files:
            logger.info(file)

        new_data_downloaded = False
        for file in list_of_files:
            file_name = os.path.basename(file)
            file_path = os.path.join(local_path, file_name)
            yadisk_file_hash = y.get_meta(file)['md5']

            if file_name not in hash_data or hash_data[file_name] != yadisk_file_hash:
                logging.info(f"Файл {file_name} изменён или новый, скачиваем его.")
                download_file(y, file, file_path)
                hash_data[file_name] = yadisk_file_hash
                new_data_downloaded = True
            else:
                logging.info(f"Файл {file_name} не изменён, пропускаем загрузку.")

        with open(hash_path, 'w') as f:
            json.dump(hash_data, f)

        if new_data_downloaded:
            logging.info("Новые данные были скачаны. Формируем датафреймы.")
            new_orders_data, new_events_data = create_datasets(local_path)

            if new_orders_data is not None or new_events_data is not None:
                logging.info("Загрузка данных в базу.")
                load_to_database(engine, new_orders_data, new_events_data)
                logging.info("Формируем витрины данных.")
                rfm_analysis(engine)
                cohort_analysis(engine)
                calculate_cdr(engine)
                transpon(engine)
            else:
                logging.warning("Нет данных для загрузки в базу.")

            clean_local_files(list_of_files, local_path)
        else:
            logging.info("Новых файлов для загрузки нет.")
    except Exception as e:
        logging.error(f"Ошибка в процессе: {str(e)}")

def main():
    
    local_path = config['local_path']
    hash_path = config['hash_path']
    
    check_token()
    extract_and_transform(yadisk_client, local_path, hash_path, engine)
    shutdown()

if __name__ == "__main__":
    main()
