import logging
from sqlalchemy import text
from utils import terminate_script 

logger = logging.getLogger()

def load_to_database(engine, new_orders_data, new_events_data):
    """Основная функция для загрузки данных в базу."""
    try:
        load_orders(engine, new_orders_data)
        load_events(engine, new_events_data)
        check_duplicates(engine)
        create_indexes(engine)
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных или создании индексов: {e}")
        terminate_script()

def load_orders(engine, new_orders_data):
    """Загружает orders в базу."""
    if new_orders_data is not None:
        logger.info("Загружаем orders в базу данных..")
        new_orders_data.to_sql('orders', engine, if_exists='append', index=False)
        logger.info("Данные из orders загружены.")
    else:
        logger.info("Нет данных для загрузки orders.")

def load_events(engine, new_events_data):
    """Загружает events в базу."""
    if new_events_data is not None:
        logger.info("Загружаем events в базу данных..")
        new_events_data.to_sql('events', engine, if_exists='append', index=False)
        logger.info("Данные из events загружены.")
    else:
        logger.info("Нет данных для загрузки events.")

def check_duplicates(engine):
    """Проверяет наличие дубликатов в таблицах orders и events."""
    with engine.connect() as conn:
        orders_duplicates = conn.execute(text(""" 
            SELECT "OrderIdsMindboxId", COUNT(*) 
            FROM orders 
            GROUP BY "OrderIdsMindboxId" 
            HAVING COUNT(*) > 1;
        """)).fetchall()
        
        if orders_duplicates:
            logger.warning("Дубликаты в таблице orders!")
        else:
            logger.info("Дубликатов в таблице orders не найдено.")

        events_duplicates = conn.execute(text(""" 
            SELECT "CustomerActionIdsMindboxId", COUNT(*) 
            FROM events 
            GROUP BY "CustomerActionIdsMindboxId" 
            HAVING COUNT(*) > 1;
        """)).fetchall()
        
        if events_duplicates:
            logger.warning("Дубликаты в таблице events!")
        else:
            logger.info("Дубликатов в таблице events не найдено.")

def create_indexes(engine):
    """Создает индексы для таблиц orders и events."""
    create_orders_index_query = """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_mindbox_id 
    ON orders ("OrderIdsMindboxId");
    """
    create_events_index_query = """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_events_mindbox_id 
    ON events ("CustomerActionIdsMindboxId");
    """   
    with engine.connect() as conn:
        conn.execute(text(create_orders_index_query))
        conn.execute(text(create_events_index_query))

    logger.info("Индексы успешно созданы.")
        
def execute_query(
    engine, 
    query, 
    success_message="", 
    error_message="", 
    fetch_results=False, 
    retries=3, 
    retry_delay=5
):
    """Выполняет запрос в базу."""
    attempt = 0
    while attempt < retries:
        try:
            with engine.begin() as conn:
                result = conn.execute(text(query))  
                if fetch_results:
                    data = result.fetchall()
                    columns = result.keys()
                    if success_message:  
                        logger.info(success_message)
                    return data, columns
                if success_message: 
                    logger.info(success_message)
            break  
        except Exception as e:
            attempt += 1
            if attempt < retries:
                logger.error(f"{error_message}: {e}. Попытка {attempt} из {retries}. "
                             f"Повторное подключение через {retry_delay} секунд...")
                time.sleep(retry_delay)  
            else:
                logger.error(f"{error_message}: {e}. Превышено количество попыток ({retries}). Операция прервана.")
                return None, None if fetch_results else None



