import os
import sys
import hashlib
import logging
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path

logger = logging.getLogger()

def calculate_file_hash(file_path):
    """Считает хеш."""
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def download_file(y, file, file_path):
    """Качает файлы с Яндекс Диска."""
    try:
        download_link = y.get_download_link(file)
        response = requests.get(download_link, stream=True)
        total_size = int(response.headers.get('content-length', 0))

        with open(file_path, 'wb') as f:
            with tqdm(total=total_size, 
                      unit='B', 
                      unit_scale=True, 
                      desc=f"Скачивание {os.path.basename(file)}") as pbar:
                for chunk in response.iter_content(1024):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
    except Exception as e:
        logger.error(f"Ошибка при скачивании файла {file}: {str(e)}")
        terminate_script()

def read_csv_file(file_path):
    """Читает CSV файлы."""
    try:
        return pd.read_csv(
            file_path,
            sep=';',
            low_memory=False,
            on_bad_lines='skip'
        )
    except pd.errors.ParserError as e:
        logger.error(f"Ошибка при чтении {file_path}: {e}")
        return None
    
def create_datasets(local_path):
    """Объединяет файлы событий в один датасет."""
    event_dataframes = []
    new_orders_data = None
    path = Path(local_path)

    for file in path.glob("*.csv"): 
        if file.name == 'orders.csv':
            new_orders_data = read_csv_file(file)
            if new_orders_data is not None:
                logger.info(f"Обработаны данные из {file.name} - {len(new_orders_data)} записей")
        else:
            events = read_csv_file(file)
            if events is not None:
                event_dataframes.append(events)
                logger.info(f"Обработаны данные из {file.name} - {len(events)} записей")

    if event_dataframes:
        new_events_data = pd.concat(event_dataframes, ignore_index=True)
        logger.info(f"Объединенный датафрейм для events загружен. Всего записей: {len(new_events_data)}")
    else:
        new_events_data = None
        logger.info("Нет загруженных файлов для events.")

    if new_orders_data is not None:
        logger.info(f"Количество строк в заказах: {len(new_orders_data)}")

    return new_orders_data, new_events_data
        
def clean_local_files(list_of_files, local_path):
    """Удаляет скачанные файлы с локального диска после обработки."""
    for file in list_of_files:
        file_path = os.path.join(local_path, os.path.basename(file))
        if os.path.exists(file_path):
            os.remove(file_path)
            
def terminate_script():
    """Завершает выполнение скрипта с сообщением об ошибке."""
    logger.error("Операция прервана! Нужно очистить базу, проверить данные и повторить операцию.")
    input("Нажмите Enter, чтобы выйти..")
    sys.exit(1)

def shutdown():
    """Завершает выполнение скрипта с сообщением об успешном завершении."""
    logging.info("Обработка успешно завершена! Нажмите Enter для выхода.")
    input() 
    sys.exit(0)