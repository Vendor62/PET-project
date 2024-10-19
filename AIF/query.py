import logging
from sqlalchemy import text
from database import execute_query

logger = logging.getLogger()

def rfm_analysis(
    engine, 
    input_table='orders', 
    output_table='rfm', 
    date_format='DD.MM.YYYY HH24:MI'
    ):
    """Формирует таблицу RFM."""

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        customer_id BIGINT,
        recency_days INT,
        frequency INT,
        monetary NUMERIC,
        recency_score INT,
        frequency_score INT,
        monetary_score INT,
        rfm_group TEXT,
        percent_rfm NUMERIC,
        cats TEXT, -- Добавляем новый столбец cats
        PRIMARY KEY (customer_id)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы RFM"
    )
    insert_rfm_query = f"""
    INSERT INTO {output_table} (
        customer_id, 
        recency_days, 
        frequency, 
        monetary, 
        recency_score, 
        frequency_score, 
        monetary_score, 
        rfm_group, 
        percent_rfm,
        cats
    )
    WITH rfm_data AS (
        SELECT
            "OrderCustomerIdsMindboxId" AS customer_id,
            MAX(TO_TIMESTAMP(
                "OrderFirstActionDateTimeUtc", 
                '{date_format}')) 
                    AS last_order_date,
            EXTRACT(DAY FROM (
                CURRENT_DATE - MAX(TO_TIMESTAMP(
                    "OrderFirstActionDateTimeUtc", '{date_format}')))) 
                        AS recency_days,
            COUNT(*) AS frequency,
            SUM("OrderTotalPrice") AS monetary
        FROM 
            {input_table}
        WHERE 
            "OrderLineStatusIdsExternalId" = 'Paid'
        GROUP BY 
            "OrderCustomerIdsMindboxId"
    ),
    rfm_scores AS (
        SELECT
            customer_id,
            recency_days,
            frequency,
            monetary,
            CASE
                WHEN recency_days <= (SELECT AVG(
                    recency_days) FROM rfm_data) - (SELECT STDDEV(
                        recency_days) FROM rfm_data) THEN 4
                WHEN recency_days <= (SELECT AVG(
                    recency_days) FROM rfm_data) THEN 3
                WHEN recency_days <= (SELECT AVG(
                    recency_days) FROM rfm_data) + (SELECT STDDEV(
                        recency_days) FROM rfm_data) THEN 2
                ELSE 1
            END AS recency_score,
            CASE
                WHEN frequency >= (SELECT AVG(
                    frequency) FROM rfm_data) + (SELECT STDDEV(
                        frequency) FROM rfm_data) THEN 4
                WHEN frequency >= (SELECT AVG(
                    frequency) FROM rfm_data) THEN 3
                WHEN frequency >= (SELECT AVG(
                    frequency) FROM rfm_data) - (SELECT STDDEV(
                        frequency) FROM rfm_data) THEN 2
                ELSE 1
            END AS frequency_score,
            CASE
                WHEN monetary >= (SELECT AVG(
                    monetary) FROM rfm_data) + (SELECT STDDEV(
                        monetary) FROM rfm_data) THEN 4
                WHEN monetary >= (SELECT AVG(
                    monetary) FROM rfm_data) THEN 3
                WHEN monetary >= (SELECT AVG(
                    monetary) FROM rfm_data) - (SELECT STDDEV(
                        monetary) FROM rfm_data) THEN 2
                ELSE 1
            END AS monetary_score
        FROM
            rfm_data
    ),
    rfm_grouped AS (
        SELECT
            customer_id,
            recency_days,
            frequency,
            monetary,
            recency_score,
            frequency_score,
            monetary_score,
            CONCAT(
                recency_score, 
                frequency_score, 
                monetary_score
    ) AS rfm_group
        FROM
            rfm_scores
    ),
    rfm_percentages AS (
        SELECT
            rfm_group,
            COUNT(*) AS group_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) 
                AS percent_rfm 
        FROM
            rfm_grouped
        GROUP BY
            rfm_group
    )
    SELECT
        rg.customer_id,
        rg.recency_days,
        rg.frequency,
        rg.monetary,
        rg.recency_score,
        rg.frequency_score,
        rg.monetary_score,
        rg.rfm_group,
        rp.percent_rfm,
        CASE
            WHEN rg.rfm_group IN (
                '444', '443', '344') 
                    THEN 'VIP'
            WHEN rg.rfm_group IN (
                '442', '441', '434', '433', '432', 
                '331', '332', '343', '342', '334') 
                    THEN 'Постоянные'
            WHEN rg.rfm_group IN (
                '431', '424', '423', '422', '421', 
                '414', '413', '412', '411') 
                    THEN 'Новые'
            WHEN rg.rfm_group IN (
                '341', '333', '324', '323', '322', 
                '243') 
                    THEN 'Высокий потенциал'
            WHEN rg.rfm_group IN (
                '321', '314', '313', '312', '311', 
                '241', '232', '222', '221', '214', 
                '213', '212', '211', '142', '141') 
                    THEN 'Малоактивные'
            WHEN rg.rfm_group IN (
                '234', '233', '242', '244', '144', 
                '224', '223', '143') 
                    THEN 'Спящие'
            WHEN rg.rfm_group IN (
                '134', '133', '132', 
                '131', '124', '123', 
                '122', '121', '114', 
                '113', '112', '111') 
                    THEN 'Потерянные'
            ELSE NULL
        END AS cats
    FROM
        rfm_grouped rg
    LEFT JOIN
        rfm_percentages rp ON rg.rfm_group = rp.rfm_group
    ON CONFLICT (customer_id) DO UPDATE SET
        recency_days = EXCLUDED.recency_days,
        frequency = EXCLUDED.frequency,
        monetary = EXCLUDED.monetary,
        recency_score = EXCLUDED.recency_score,
        frequency_score = EXCLUDED.frequency_score,
        monetary_score = EXCLUDED.monetary_score,
        rfm_group = EXCLUDED.rfm_group,
        percent_rfm = EXCLUDED.percent_rfm,
        cats = EXCLUDED.cats;  
    """
    execute_query(
        engine,
        insert_rfm_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу RFM"
    )

    
def calculate_cohorts_all(engine, 
                           input_table='events', 
                           output_table='cohorts_all', 
                           date_format='DD.MM.YYYY HH24:MI'):
    """Формирует таблицу с когортами пользователей по месяцу первого действия."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        user_id BIGINT,
        cohort_month DATE,
        PRIMARY KEY (user_id, cohort_month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы cohorts_all"
    )
    insert_cohorts_query = f"""
    INSERT INTO {output_table} (user_id, cohort_month)
    WITH cohort AS (
        SELECT 
            "CustomerActionCustomerIdsMindboxId" AS user_id,
            DATE_TRUNC(
                'month', 
                MIN(to_timestamp("CustomerActionDateTimeUtc", 
                '{date_format}'))) AS cohort_month
        FROM 
            {input_table}
        GROUP BY 
            "CustomerActionCustomerIdsMindboxId"
    )
    SELECT 
        user_id, 
        cohort_month
    FROM cohort
    ON CONFLICT (user_id, cohort_month) DO NOTHING;  
    """
    execute_query(
        engine,
        insert_cohorts_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу cohorts_all"
    )    
    
def calculate_cohorts_paid(engine, 
                           input_table='orders', 
                           output_table='cohorts_paid', 
                           date_format='DD.MM.YYYY HH24:MI'):
    """Формирует таблицу с когортами платящих пользователей по месяцу первого платежа."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        user_id BIGINT,
        cohort_month DATE,
        PRIMARY KEY (user_id, cohort_month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы cohorts_paid"
    )
    insert_cohorts_query = f"""
    INSERT INTO {output_table} (user_id, cohort_month)
    WITH cohort AS (
        SELECT 
            "OrderCustomerIdsMindboxId" AS user_id,
            DATE_TRUNC(
                'month', 
                MIN(to_timestamp("OrderFirstActionDateTimeUtc", 
                '{date_format}'))) AS cohort_month
        FROM 
            {input_table}
        WHERE 
            "OrderLineStatusIdsExternalId" = 'Paid'
        GROUP BY 
            "OrderCustomerIdsMindboxId"
    )
    SELECT 
        user_id, 
        cohort_month
    FROM cohort
    ON CONFLICT (user_id, cohort_month) DO NOTHING; 
    """
    execute_query(
        engine,
        insert_cohorts_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу cohorts_paid"
    )   
     
def calculate_arppu(engine, 
                     output_table='cohorts_revenue_arppu_data', 
                     date_format='DD.MM.YYYY HH24:MI'):
    """Формирует таблицу с данными по выручке и ARPPU."""    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        total_users BIGINT,
        revenue_month_12 NUMERIC,
        revenue_month_11 NUMERIC,
        revenue_month_10 NUMERIC,
        revenue_month_9 NUMERIC,
        revenue_month_8 NUMERIC,
        revenue_month_7 NUMERIC,
        revenue_month_6 NUMERIC,
        revenue_month_5 NUMERIC,
        revenue_month_4 NUMERIC,
        revenue_month_3 NUMERIC,
        revenue_month_2 NUMERIC,
        revenue_month_1 NUMERIC,
        arppu_month_12 NUMERIC,
        arppu_month_11 NUMERIC,
        arppu_month_10 NUMERIC,
        arppu_month_9 NUMERIC,
        arppu_month_8 NUMERIC,
        arppu_month_7 NUMERIC,
        arppu_month_6 NUMERIC,
        arppu_month_5 NUMERIC,
        arppu_month_4 NUMERIC,
        arppu_month_3 NUMERIC,
        arppu_month_2 NUMERIC,
        arppu_month_1 NUMERIC,
        PRIMARY KEY (cohort_month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы cohorts_revenue_arppu_data"
    )
    insert_cohorts_query = f"""
    INSERT INTO {output_table} (
        cohort_month, total_users, 
        revenue_month_12, revenue_month_11, 
        revenue_month_10, revenue_month_9, 
        revenue_month_8, revenue_month_7, 
        revenue_month_6, revenue_month_5, 
        revenue_month_4, revenue_month_3, 
        revenue_month_2, revenue_month_1, 
        arppu_month_12, arppu_month_11, 
        arppu_month_10, arppu_month_9, 
        arppu_month_8, arppu_month_7, 
        arppu_month_6, arppu_month_5, 
        arppu_month_4, arppu_month_3, 
        arppu_month_2, arppu_month_1)
    WITH cohort AS (
        SELECT 
            "OrderCustomerIdsMindboxId" AS user_id,
            DATE_TRUNC(
                'month', 
                MIN(to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}'))) AS cohort_month
        FROM 
            orders
        WHERE 
            "OrderLineStatusIdsExternalId" = 'Paid'
        GROUP BY 
            "OrderCustomerIdsMindboxId"
    ),
    user_counts AS (
        SELECT 
            cohort_month,
            COUNT(user_id) AS total_users
        FROM cohort
        GROUP BY cohort_month
    ),
    monthly_revenue AS (
        SELECT 
            c.cohort_month,
            DATE_TRUNC(
                'month', to_timestamp(
                    o."OrderFirstActionDateTimeUtc", 
                    '{date_format}')) AS month,
            SUM(o."OrderTotalPrice") AS total_revenue
        FROM cohort c
        JOIN orders o ON c.user_id = o."OrderCustomerIdsMindboxId"
        WHERE o."OrderLineStatusIdsExternalId"::text = 'Paid'
        GROUP BY c.cohort_month, month
    )
    SELECT 
        u.cohort_month,
        u.total_users,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '11 months' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_12,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '10 months' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_11,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '9 months' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_10,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '8 months' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_9,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '7 months' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_8,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '6 months' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_7,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '5 months' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_6,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '4 months' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_5,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '3 months' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_4,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '2 months' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_3,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '1 month' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_2,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '0 month' THEN 
                m.total_revenue ELSE 0 END) AS revenue_month_1,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '11 months' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_12,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '10 months' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_11,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '9 months' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_10,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '8 months' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_9,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '7 months' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_8,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '6 months' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_7,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '5 months' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_6,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '4 months' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_5,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '3 months' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_4,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '2 months' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_3,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '1 month' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_2,
        SUM(CASE WHEN m.month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '0 month' THEN 
                m.total_revenue ELSE 0 END) / COALESCE(NULLIF(
                    u.total_users, 0), 1) AS arppu_month_1
    FROM user_counts u
    LEFT JOIN monthly_revenue m ON u.cohort_month = m.cohort_month
    GROUP BY u.cohort_month, u.total_users
    ORDER BY u.cohort_month
    ON CONFLICT (cohort_month) DO NOTHING; 
    """
    execute_query(
        engine,
        insert_cohorts_query,
        f"Данные по выручке и ARPPU успешно сохранены в таблице {output_table}.",
        "Ошибка при сохранении данных по выручке и ARPPU в таблицу"
    )

def calculate_arppu_cumulative(engine, 
                             input_table='cohorts_revenue_arppu_data', 
                             output_table='arppu_cumulative'):
    """Формирует таблицу с кумулятивным ARPPU."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        total_users BIGINT,
        cumulative_arppu_12 NUMERIC,
        cumulative_arppu_11 NUMERIC,
        cumulative_arppu_10 NUMERIC,
        cumulative_arppu_9 NUMERIC,
        cumulative_arppu_8 NUMERIC,
        cumulative_arppu_7 NUMERIC,
        cumulative_arppu_6 NUMERIC,
        cumulative_arppu_5 NUMERIC,
        cumulative_arppu_4 NUMERIC,
        cumulative_arppu_3 NUMERIC,
        cumulative_arppu_2 NUMERIC,
        cumulative_arppu_1 NUMERIC,
        PRIMARY KEY (cohort_month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы arppu_cumulative"
    )
    insert_arppu_query = f"""
    INSERT INTO {output_table} (
        cohort_month, total_users, 
        cumulative_arppu_12, cumulative_arppu_11, 
        cumulative_arppu_10, cumulative_arppu_9, 
        cumulative_arppu_8, cumulative_arppu_7, 
        cumulative_arppu_6, cumulative_arppu_5, 
        cumulative_arppu_4, cumulative_arppu_3, 
        cumulative_arppu_2, cumulative_arppu_1
    )
    WITH cumulative_arppu AS (
        SELECT 
            cohort_month,
            ROUND("arppu_month_12", 2) AS "ARPPU Month 12",
            ROUND("arppu_month_11" + "arppu_month_12", 2) 
                AS cumulative_arppu_11,
            ROUND("arppu_month_10" + "arppu_month_11" + 
                "arppu_month_12", 2) AS cumulative_arppu_10,
            ROUND("arppu_month_9" + "arppu_month_10" + 
                "arppu_month_11" + "arppu_month_12", 2) 
                    AS cumulative_arppu_9,
            ROUND("arppu_month_8" + "arppu_month_9" + 
                "arppu_month_10" + "arppu_month_11" + 
                        "arppu_month_12", 2) 
                        AS cumulative_arppu_8,
            ROUND("arppu_month_7" + "arppu_month_8" + 
                "arppu_month_9" + "arppu_month_10" + 
                    "arppu_month_11" + "arppu_month_12", 2) 
                        AS cumulative_arppu_7,
            ROUND("arppu_month_6" + "arppu_month_7" + 
                "arppu_month_8" + "arppu_month_9" + 
                    "arppu_month_10" + "arppu_month_11" + 
                        "arppu_month_12", 2) 
                            AS cumulative_arppu_6,
            ROUND("arppu_month_5" + "arppu_month_6" + 
                "arppu_month_7" + "arppu_month_8" + 
                    "arppu_month_9" + "arppu_month_10" + 
                        "arppu_month_11" + "arppu_month_12", 2) 
                            AS cumulative_arppu_5,
            ROUND("arppu_month_4" + "arppu_month_5" + 
                "arppu_month_6" + "arppu_month_7" + "arppu_month_8" + 
                    "arppu_month_9" + "arppu_month_10" + 
                        "arppu_month_11" + "arppu_month_12", 2) 
                            AS cumulative_arppu_4,
            ROUND("arppu_month_3" + "arppu_month_4" + "arppu_month_5" + 
                "arppu_month_6" + "arppu_month_7" + "arppu_month_8" + 
                    "arppu_month_9" + "arppu_month_10" + 
                        "arppu_month_11" + "arppu_month_12", 2) 
                            AS cumulative_arppu_3,
            ROUND("arppu_month_2" + "arppu_month_3" + "arppu_month_4" + 
                "arppu_month_5" + "arppu_month_6" + "arppu_month_7" + 
                    "arppu_month_8" + "arppu_month_9" + 
                        "arppu_month_10" + "arppu_month_11" + 
                            "arppu_month_12", 2) AS cumulative_arppu_2,
            ROUND("arppu_month_1" + "arppu_month_2" + "arppu_month_3" + 
                "arppu_month_4" + "arppu_month_5" + "arppu_month_6" + 
                    "arppu_month_7" + "arppu_month_8" + 
                        "arppu_month_9" + "arppu_month_10" + 
                            "arppu_month_11" + "arppu_month_12", 2) 
                                AS cumulative_arppu_1,
            total_users
        FROM {input_table}
    )
    SELECT 
        cohort_month,
        total_users,
        ROUND("ARPPU Month 12", 2) AS cumulative_arppu_12,
        ROUND(cumulative_arppu_11, 2) AS cumulative_arppu_11,
        ROUND(cumulative_arppu_10, 2) AS cumulative_arppu_10,
        ROUND(cumulative_arppu_9, 2) AS cumulative_arppu_9,
        ROUND(cumulative_arppu_8, 2) AS cumulative_arppu_8,
        ROUND(cumulative_arppu_7, 2) AS cumulative_arppu_7,
        ROUND(cumulative_arppu_6, 2) AS cumulative_arppu_6,
        ROUND(cumulative_arppu_5, 2) AS cumulative_arppu_5,
        ROUND(cumulative_arppu_4, 2) AS cumulative_arppu_4,
        ROUND(cumulative_arppu_3, 2) AS cumulative_arppu_3,
        ROUND(cumulative_arppu_2, 2) AS cumulative_arppu_2,
        ROUND(cumulative_arppu_1, 2) AS cumulative_arppu_1
    FROM cumulative_arppu;
    """
    execute_query(
        engine,
        insert_arppu_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу arppu_cumulative"
    )  

def calculate_ltv_cohorts(engine, 
                          input_table_1='cohorts_all', 
                          input_table_2='cohorts_revenue_arppu_data', 
                          output_table='ltv_cohorts'):
    """Формирует таблицу с выручкой и LTV."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        total_users INTEGER,
        revenue_month_12 NUMERIC(10, 2),
        revenue_month_11 NUMERIC(10, 2),
        revenue_month_10 NUMERIC(10, 2),
        revenue_month_9 NUMERIC(10, 2),
        revenue_month_8 NUMERIC(10, 2),
        revenue_month_7 NUMERIC(10, 2),
        revenue_month_6 NUMERIC(10, 2),
        revenue_month_5 NUMERIC(10, 2),
        revenue_month_4 NUMERIC(10, 2),
        revenue_month_3 NUMERIC(10, 2),
        revenue_month_2 NUMERIC(10, 2),
        revenue_month_1 NUMERIC(10, 2),
        ltv_month_12 NUMERIC(10, 2),
        ltv_month_11 NUMERIC(10, 2),
        ltv_month_10 NUMERIC(10, 2),
        ltv_month_9 NUMERIC(10, 2),
        ltv_month_8 NUMERIC(10, 2),
        ltv_month_7 NUMERIC(10, 2),
        ltv_month_6 NUMERIC(10, 2),
        ltv_month_5 NUMERIC(10, 2),
        ltv_month_4 NUMERIC(10, 2),
        ltv_month_3 NUMERIC(10, 2),
        ltv_month_2 NUMERIC(10, 2),
        ltv_month_1 NUMERIC(10, 2),
        PRIMARY KEY (cohort_month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы ltv_cohorts"
    )
    insert_ltv_query = f"""
    INSERT INTO {output_table} (
        cohort_month, total_users, revenue_month_12, 
        revenue_month_11, revenue_month_10,
        revenue_month_9, revenue_month_8, revenue_month_7, 
        revenue_month_6, revenue_month_5,
        revenue_month_4, revenue_month_3, revenue_month_2, 
        revenue_month_1, ltv_month_12, ltv_month_11, 
        ltv_month_10, ltv_month_9, ltv_month_8, ltv_month_7,
        ltv_month_6, ltv_month_5, ltv_month_4, ltv_month_3, 
        ltv_month_2, ltv_month_1
    )
    SELECT 
        a.cohort_month, 
        COUNT(a.user_id) AS total_users,
        COALESCE(b.revenue_month_12, 0) AS revenue_month_12,
        COALESCE(b.revenue_month_11, 0) AS revenue_month_11,
        COALESCE(b.revenue_month_10, 0) AS revenue_month_10,
        COALESCE(b.revenue_month_9, 0) AS revenue_month_9,
        COALESCE(b.revenue_month_8, 0) AS revenue_month_8,
        COALESCE(b.revenue_month_7, 0) AS revenue_month_7,
        COALESCE(b.revenue_month_6, 0) AS revenue_month_6,
        COALESCE(b.revenue_month_5, 0) AS revenue_month_5,
        COALESCE(b.revenue_month_4, 0) AS revenue_month_4,
        COALESCE(b.revenue_month_3, 0) AS revenue_month_3,
        COALESCE(b.revenue_month_2, 0) AS revenue_month_2,
        COALESCE(b.revenue_month_1, 0) AS revenue_month_1,
        ROUND(COALESCE(b.revenue_month_12, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_12,
        ROUND(COALESCE(b.revenue_month_11, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_11,
        ROUND(COALESCE(b.revenue_month_10, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_10,
        ROUND(COALESCE(b.revenue_month_9, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_9,
        ROUND(COALESCE(b.revenue_month_8, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_8,
        ROUND(COALESCE(b.revenue_month_7, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_7,
        ROUND(COALESCE(b.revenue_month_6, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_6,
        ROUND(COALESCE(b.revenue_month_5, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_5,
        ROUND(COALESCE(b.revenue_month_4, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_4,
        ROUND(COALESCE(b.revenue_month_3, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_3,
        ROUND(COALESCE(b.revenue_month_2, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_2,
        ROUND(COALESCE(b.revenue_month_1, 0) / 
            NULLIF(COUNT(a.user_id), 0), 2) AS ltv_month_1
    FROM 
        {input_table_1} a
    LEFT JOIN 
        {input_table_2} b 
        ON a.cohort_month = b.cohort_month
    GROUP BY 
        a.cohort_month, 
        b.revenue_month_12, 
        b.revenue_month_11, 
        b.revenue_month_10, 
        b.revenue_month_9, 
        b.revenue_month_8, 
        b.revenue_month_7, 
        b.revenue_month_6, 
        b.revenue_month_5, 
        b.revenue_month_4, 
        b.revenue_month_3, 
        b.revenue_month_2, 
        b.revenue_month_1
    ORDER BY 
        a.cohort_month
    ON CONFLICT (cohort_month) DO NOTHING;
    """ 
    execute_query(
        engine, 
        insert_ltv_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу ltv_cohorts"
    )

def calculate_cumulative_ltv(engine, 
                             input_table='ltv_cohorts', 
                             output_table='cumulative_ltv', 
                             date_format='DD.MM.YYYY HH24:MI'):
    """Считает кумулятивную сумму LTV."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        total_users BIGINT,
        ltv_cumulative_12 NUMERIC(10, 2),
        ltv_cumulative_11 NUMERIC(10, 2),
        ltv_cumulative_10 NUMERIC(10, 2),
        ltv_cumulative_9 NUMERIC(10, 2),
        ltv_cumulative_8 NUMERIC(10, 2),
        ltv_cumulative_7 NUMERIC(10, 2),
        ltv_cumulative_6 NUMERIC(10, 2),
        ltv_cumulative_5 NUMERIC(10, 2),
        ltv_cumulative_4 NUMERIC(10, 2),
        ltv_cumulative_3 NUMERIC(10, 2),
        ltv_cumulative_2 NUMERIC(10, 2),
        ltv_cumulative_1 NUMERIC(10, 2),
        PRIMARY KEY (cohort_month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы cumulative_ltv"
    )
    insert_ltv_query = f"""
    INSERT INTO {output_table} (
        cohort_month, total_users, ltv_cumulative_12, 
        ltv_cumulative_11, ltv_cumulative_10, 
        ltv_cumulative_9, ltv_cumulative_8, 
        ltv_cumulative_7, ltv_cumulative_6, 
        ltv_cumulative_5, ltv_cumulative_4, 
        ltv_cumulative_3, ltv_cumulative_2, 
        ltv_cumulative_1
    )
    SELECT 
        cohort_month,
        total_users,
        ltv_month_12 AS ltv_cumulative_12,
        (ltv_month_12 + ltv_month_11) AS ltv_cumulative_11,
        (ltv_month_12 + ltv_month_11 + ltv_month_10) 
            AS ltv_cumulative_10,
        (ltv_month_12 + ltv_month_11 + ltv_month_10 + 
            ltv_month_9) AS ltv_cumulative_9,
        (ltv_month_12 + ltv_month_11 + ltv_month_10 + 
            ltv_month_9 + ltv_month_8) AS ltv_cumulative_8,
        (ltv_month_12 + ltv_month_11 + ltv_month_10 + 
            ltv_month_9 + ltv_month_8 + ltv_month_7) 
                AS ltv_cumulative_7,
        (ltv_month_12 + ltv_month_11 + ltv_month_10 + 
            ltv_month_9 + ltv_month_8 + ltv_month_7 + 
                ltv_month_6) AS ltv_cumulative_6,
        (ltv_month_12 + ltv_month_11 + ltv_month_10 + 
            ltv_month_9 + ltv_month_8 + ltv_month_7 + 
                ltv_month_6 + ltv_month_5) 
                    AS ltv_cumulative_5,
        (ltv_month_12 + ltv_month_11 + ltv_month_10 + 
            ltv_month_9 + ltv_month_8 + ltv_month_7 + 
                ltv_month_6 + ltv_month_5 + ltv_month_4) 
                    AS ltv_cumulative_4,
        (ltv_month_12 + ltv_month_11 + ltv_month_10 + 
            ltv_month_9 + ltv_month_8 + ltv_month_7 + 
                ltv_month_6 + ltv_month_5 + ltv_month_4 + 
                    ltv_month_3) AS ltv_cumulative_3,
        (ltv_month_12 + ltv_month_11 + ltv_month_10 + 
            ltv_month_9 + ltv_month_8 + ltv_month_7 + 
                ltv_month_6 + ltv_month_5 + ltv_month_4 + 
                    ltv_month_3 + ltv_month_2) 
                        AS ltv_cumulative_2,
        (ltv_month_12 + ltv_month_11 + ltv_month_10 + 
            ltv_month_9 + ltv_month_8 + ltv_month_7 + 
                ltv_month_6 + ltv_month_5 + ltv_month_4 + 
                    ltv_month_3 + ltv_month_2 + ltv_month_1) 
                        AS ltv_cumulative_1
    FROM {input_table}
    ORDER BY cohort_month;
    """
    execute_query(
        engine,
        insert_ltv_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу cumulative_ltv"
    )
    
def calculate_rr(engine, 
                  input_table='orders', 
                  output_table='rr_cohorts', 
                  date_format='DD.MM.YYYY HH24:MI:SS'):
    """Считает RR для когорт."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort TEXT,
        total_users BIGINT,
        "RR Month 12" NUMERIC(5, 2),
        "RR Month 11" NUMERIC(5, 2),
        "RR Month 10" NUMERIC(5, 2),
        "RR Month 9" NUMERIC(5, 2),
        "RR Month 8" NUMERIC(5, 2),
        "RR Month 7" NUMERIC(5, 2),
        "RR Month 6" NUMERIC(5, 2),
        "RR Month 5" NUMERIC(5, 2),
        "RR Month 4" NUMERIC(5, 2),
        "RR Month 3" NUMERIC(5, 2),
        "RR Month 2" NUMERIC(5, 2),
        "RR Month 1" NUMERIC(5, 2),
        PRIMARY KEY (cohort)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы rr_cohorts"
    )
    insert_retention_rate_query = f"""
    WITH cohort AS (
        SELECT 
            "OrderCustomerIdsMindboxId" AS user_id,
            DATE_TRUNC('month', MIN(to_timestamp(
                "OrderFirstActionDateTimeUtc", 
                '{date_format}'))) AS cohort_month
        FROM {input_table}
        WHERE "OrderLineStatusIdsExternalId" = 'Paid'
        GROUP BY user_id
    ),
    active_users AS (
        SELECT 
            "OrderCustomerIdsMindboxId",
            DATE_TRUNC('month', to_timestamp(
                "OrderFirstActionDateTimeUtc", 
                '{date_format}')) AS active_month
        FROM {input_table}
        WHERE "OrderLineStatusIdsExternalId" = 'Paid'
    ),
    user_counts AS (
        SELECT 
            cohort_month,
            COUNT(user_id) AS total_users
        FROM cohort
        GROUP BY cohort_month
    )
    INSERT INTO {output_table} (
        cohort, total_users, "RR Month 12", "RR Month 11", 
        "RR Month 10", "RR Month 9", "RR Month 8", 
        "RR Month 7", "RR Month 6", "RR Month 5", 
        "RR Month 4", "RR Month 3", "RR Month 2", 
        "RR Month 1"
    )
    SELECT 
        TO_CHAR(c.cohort_month, 'YYYY-MM') AS cohort,
        u.total_users,
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '11 months' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 12",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '10 months' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 11",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '9 months' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 10",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '8 months' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 9",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '7 months' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 8",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '6 months' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 7",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '5 months' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 6",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '4 months' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 5",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '3 months' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 4",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '2 months' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 3",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '1 month' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 2",
        ROUND(COUNT(DISTINCT CASE WHEN a.active_month = DATE_TRUNC(
            'month', CURRENT_DATE) - INTERVAL '0 month' THEN 
                a."OrderCustomerIdsMindboxId" END) * 100.0 / 
                    NULLIF(u.total_users, 0), 2) AS "RR Month 1"
    FROM cohort c
    LEFT JOIN active_users a ON c.user_id = a."OrderCustomerIdsMindboxId"
    JOIN user_counts u ON c.cohort_month = u.cohort_month
    GROUP BY c.cohort_month, u.total_users
    ORDER BY c.cohort_month;
    """
    execute_query(
        engine,
        insert_retention_rate_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу rr_cohorts"
    )
    
def calculate_ac(engine, 
                 input_table='orders', 
                 output_table='ac_cohort', 
                 date_format='DD.MM.YYYY HH24:MI:SS'):
    """Формирует таблицу со средним чеком."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort VARCHAR(7),
        unique_users BIGINT,
        average_check_current_month NUMERIC,
        average_check_month_1 NUMERIC,
        average_check_month_2 NUMERIC,
        average_check_month_3 NUMERIC,
        average_check_month_4 NUMERIC,
        average_check_month_5 NUMERIC,
        average_check_month_6 NUMERIC,
        average_check_month_7 NUMERIC,
        average_check_month_8 NUMERIC,
        average_check_month_9 NUMERIC,
        average_check_month_10 NUMERIC,
        average_check_month_11 NUMERIC,
        PRIMARY KEY (cohort)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы ac_cohort"
    )
    insert_revenue_query = f"""
    INSERT INTO {output_table} (
        cohort, unique_users, 
        average_check_current_month, 
        average_check_month_1, average_check_month_2, 
        average_check_month_3, average_check_month_4, 
        average_check_month_5, average_check_month_6, 
        average_check_month_7, average_check_month_8, 
        average_check_month_9, average_check_month_10, 
        average_check_month_11
    )
    WITH cohort AS (
    SELECT 
            "OrderCustomerIdsMindboxId" AS user_id,
            DATE_TRUNC('month', 
            MIN(to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}'))) AS cohort_month
        FROM {input_table}
        WHERE "OrderLineStatusIdsExternalId" = 'Paid'
        GROUP BY user_id
    )
    SELECT 
        TO_CHAR(cohort_month, 'YYYY-MM') AS cohort,
        COUNT(DISTINCT user_id) AS unique_users,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE) THEN 
                "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE) 
                        THEN 1 END), 0), 2) 
                            AS average_check_current_month,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '1 month') THEN 
                "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '1 month') 
                        THEN 1 END), 0), 2) 
                            AS average_check_month_1,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '2 months') THEN 
                "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '2 months') 
                        THEN 1 END), 0), 2) 
                            AS average_check_month_2,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '3 months') 
                THEN "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '3 months') 
                        THEN 1 END), 0), 2) 
                            AS average_check_month_3,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '4 months') 
                THEN "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '4 months') 
                        THEN 1 END), 0), 2) 
                            AS average_check_month_4,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '5 months') 
                THEN "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '5 months') 
                        THEN 1 END), 0), 2) 
                            AS average_check_month_5,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '6 months') 
                THEN "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '6 months') 
                        THEN 1 END), 0), 2) 
                            AS average_check_month_6,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '7 months') 
                THEN "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '7 months') 
                        THEN 1 END), 0), 2) 
                            AS average_check_month_7,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '8 months') 
                THEN "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '8 months') 
                        THEN 1 END), 0), 2) AS average_check_month_8,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '9 months') 
                THEN "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '9 months') 
                        THEN 1 END), 0), 2) 
                            AS average_check_month_9,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '10 months') 
                THEN "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '10 months') 
                        THEN 1 END), 0), 2) 
                            AS average_check_month_10,
        ROUND(SUM(CASE WHEN DATE_TRUNC(
            'month', 
            to_timestamp("OrderFirstActionDateTimeUtc", 
            '{date_format}')) = date_trunc(
                'month', CURRENT_DATE - INTERVAL '11 months') 
                THEN "OrderTotalPrice" ELSE 0 END) / NULLIF(COUNT(
                    CASE WHEN DATE_TRUNC('month', to_timestamp(
                    "OrderFirstActionDateTimeUtc", 
                    '{date_format}')) = date_trunc(
                        'month', CURRENT_DATE - INTERVAL '11 months') 
                        THEN 1 END), 0), 2) 
                            AS average_check_month_11
    FROM {input_table}
    JOIN cohort ON {input_table}."OrderCustomerIdsMindboxId" = cohort.user_id
    WHERE "OrderLineStatusIdsExternalId" = 'Paid'
    GROUP BY cohort_month
    ORDER BY cohort_month ASC;
    """
    execute_query(
        engine, 
        insert_revenue_query,
        "Данные успешно вставлены в таблицу."
    )
    
def calculate_cdr(engine):
    query = """
    WITH donor_activity AS (
        SELECT 
            "OrderCustomerIdsMindboxId" AS donor_id,
            DATE_TRUNC('month', to_timestamp(
                "OrderFirstActionDateTimeUtc", 
                'DD.MM.YYYY HH24:MI')) AS month,
            COUNT(*) AS donation_count
        FROM orders
        WHERE "OrderLineStatusIdsExternalId" = 'Paid'
        GROUP BY "OrderCustomerIdsMindboxId", month
    ),
    donor_status AS (
        SELECT 
            da1.month,
            COUNT(DISTINCT da1.donor_id) AS total_donors,
            COUNT(DISTINCT da2.donor_id) AS retained_donors
        FROM donor_activity da1
        LEFT JOIN donor_activity da2 
            ON da1.donor_id = da2.donor_id 
            AND da2.month > da1.month 
            AND da2.month <= da1.month + interval '3 months'
        GROUP BY da1.month
    ),
    churn_rate AS (
        SELECT 
            month,
            CASE
                WHEN total_donors > 0 THEN
                    ROUND((
                        total_donors - retained_donors) * 100.0 / 
                        total_donors, 2)
                ELSE NULL
            END AS churn_rate
        FROM donor_status
    ),
    new_donors AS (
        SELECT 
            "OrderCustomerIdsMindboxId" AS donor_id,
            DATE_TRUNC('month', to_timestamp(
                "OrderFirstActionDateTimeUtc", 
                'DD.MM.YYYY HH24:MI')) AS month
        FROM orders
        WHERE "OrderLineStatusIdsExternalId" = 'Paid'
        GROUP BY donor_id, month
    ),
    first_donors AS (
        SELECT 
            donor_id,
            MIN(month) AS first_month
        FROM new_donors
        GROUP BY donor_id
    ),
    monthly_new_donors AS (
        SELECT 
            DATE_TRUNC('month', first_month) AS month,
            COUNT(*) AS new_donors_count
        FROM first_donors
        GROUP BY month
    ),
    total_donors AS (
        SELECT 
            DATE_TRUNC('month', to_timestamp(
                "OrderFirstActionDateTimeUtc", 
                'DD.MM.YYYY HH24:MI')) AS month,
            COUNT(DISTINCT "OrderCustomerIdsMindboxId") AS total_donors_count
        FROM orders
        WHERE "OrderLineStatusIdsExternalId" = 'Paid'
        GROUP BY month
    ),
    new_donors_ratio AS (
        SELECT 
            mnd.month,
            ROUND(
                COALESCE(
                    mnd.new_donors_count, 0) * 100.0 / 
                    NULLIF(td.total_donors_count, 0), 2) AS new_donors_ratio
        FROM total_donors td
        LEFT JOIN monthly_new_donors mnd ON td.month = mnd.month
    ),
    monthly_donors AS (
        SELECT 
            DATE_TRUNC('month', to_timestamp(
                "OrderFirstActionDateTimeUtc", 
                'DD.MM.YYYY HH24:MI')) AS month,
            "OrderCustomerIdsMindboxId",
            COUNT(*) AS donation_count,
            MIN(to_timestamp(
                "OrderFirstActionDateTimeUtc", 
                'DD.MM.YYYY HH24:MI')) AS first_donation_date,
            MAX(to_timestamp(
                "OrderFirstActionDateTimeUtc", 
                'DD.MM.YYYY HH24:MI')) AS last_donation_date
        FROM orders
        WHERE "OrderLineStatusIdsExternalId" = 'Paid'
        GROUP BY month, "OrderCustomerIdsMindboxId"
        HAVING COUNT(*) > 1
    ),
    avg_days_between_donations AS (
        SELECT 
            md.month,
            ROUND(AVG(EXTRACT(
                EPOCH FROM (md.last_donation_date - md.first_donation_date)) / 
                    86400), 2) AS avg_days_between_donations
        FROM monthly_donors md
        GROUP BY md.month
    ),
    next_three_months AS (
        SELECT 
            current_month.month,
            ROUND(AVG(
                next_months.avg_days_between_donations), 2) AS avg_days_between_donations
        FROM avg_days_between_donations current_month
        LEFT JOIN avg_days_between_donations next_months
            ON next_months.month > current_month.month
            AND next_months.month <= (current_month.month + INTERVAL '3 months')
        GROUP BY current_month.month
    )
    SELECT 
        cr.month,
        cr.churn_rate,
        COALESCE(ndr.new_donors_ratio, 0) AS new_donors_ratio,
        COALESCE(n3m.avg_days_between_donations, 0) AS avg_days_between_donations
    FROM churn_rate cr
    LEFT JOIN new_donors_ratio ndr ON cr.month = ndr.month
    LEFT JOIN next_three_months n3m ON cr.month = n3m.month
    ORDER BY cr.month;
    """
    metrics_data, columns = execute_query(
        engine, query, 
        "Метрики CDR успешно рассчитаны.", 
        "Ошибка при расчете метрик!", fetch_results=True)

    if metrics_data is not None:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS cdr (
            month DATE PRIMARY KEY,
            churn_rate NUMERIC,
            new_donors_ratio NUMERIC,
            avg_days_between_donations NUMERIC
        );
        """
        execute_query(
            engine, 
            create_table_query, 
            "Таблица CDR создана.", 
            "Ошибка при создании таблицы CDR!")

        for row in metrics_data:
            insert_query = text("""
            INSERT INTO cdr (month, churn_rate, new_donors_ratio, avg_days_between_donations)
            VALUES (:month, :churn_rate, :new_donors_ratio, :avg_days_between_donations)
            ON CONFLICT (month) DO NOTHING;
            """)
            insert_data = {
                'month': row[0],
                'churn_rate': row[1],
                'new_donors_ratio': row[2],
                'avg_days_between_donations': row[3]
            }
            with engine.begin() as conn:
                conn.execute(insert_query, insert_data)
           
def transpon_ltv(
    engine, 
    input_table='ltv_cohorts', 
    output_table='ltv_t'
    ):
    """Преобразует таблицу ltv_cohorts в плоский формат."""

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        month INT,
        ltv NUMERIC,
        PRIMARY KEY (cohort_month, month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы ltv_t"
    )
    insert_ltv_query = f"""
    INSERT INTO {output_table} (cohort_month, month, ltv)
    WITH flattened_ltv AS (
        SELECT
            cohort_month,
            1 AS month,
            ltv_month_1 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            2 AS month,
            ltv_month_2 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            3 AS month,
            ltv_month_3 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            4 AS month,
            ltv_month_4 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            5 AS month,
            ltv_month_5 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            6 AS month,
            ltv_month_6 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            7 AS month,
            ltv_month_7 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            8 AS month,
            ltv_month_8 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            9 AS month,
            ltv_month_9 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            10 AS month,
            ltv_month_10 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            11 AS month,
            ltv_month_11 AS ltv
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            12 AS month,
            ltv_month_12 AS ltv
        FROM {input_table}
    )
    SELECT cohort_month, month, ltv
    FROM flattened_ltv
    ON CONFLICT (cohort_month, month) DO NOTHING;
    """
    execute_query(
        engine,
        insert_ltv_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу ltv_t"
    )
    
def transpon_revenue(
    engine, 
    input_table='ltv_cohorts', 
    output_table='revenue_t'
    ):
    """Преобразует таблицу ltv_cohorts в плоский формат."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        month INT,
        revenue NUMERIC,
        PRIMARY KEY (cohort_month, month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы revenue_t"
    )
    insert_revenue_query = f"""
    INSERT INTO {output_table} (cohort_month, month, revenue)
    WITH flattened_revenue AS (
        SELECT
            cohort_month,
            1 AS month,
            revenue_month_1 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            2 AS month,
            revenue_month_2 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            3 AS month,
            revenue_month_3 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            4 AS month,
            revenue_month_4 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            5 AS month,
            revenue_month_5 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            6 AS month,
            revenue_month_6 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            7 AS month,
            revenue_month_7 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            8 AS month,
            revenue_month_8 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            9 AS month,
            revenue_month_9 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            10 AS month,
            revenue_month_10 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            11 AS month,
            revenue_month_11 AS revenue
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            12 AS month,
            revenue_month_12 AS revenue
        FROM {input_table}
    )
    SELECT cohort_month, month, revenue
    FROM flattened_revenue
    ON CONFLICT (cohort_month, month) DO NOTHING;
    """
    execute_query(
        engine,
        insert_revenue_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу revenue_t"
    )
    
def transpon_cumulative_ltv(
    engine, 
    input_table='cumulative_ltv', 
    output_table='ltv_cum_t'
    ):
    """Преобразует таблицу cumulative_ltv в плоский формат."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        month INT,
        ltv_cumulative NUMERIC,
        PRIMARY KEY (cohort_month, month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы ltv_cum_t"
    )
    insert_ltv_cumulative_query = f"""
    INSERT INTO {output_table} (cohort_month, month, ltv_cumulative)
    WITH flattened_ltv AS (
        SELECT
            cohort_month,
            1 AS month,
            ltv_cumulative_1 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            2 AS month,
            ltv_cumulative_2 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            3 AS month,
            ltv_cumulative_3 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            4 AS month,
            ltv_cumulative_4 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            5 AS month,
            ltv_cumulative_5 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            6 AS month,
            ltv_cumulative_6 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            7 AS month,
            ltv_cumulative_7 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            8 AS month,
            ltv_cumulative_8 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            9 AS month,
            ltv_cumulative_9 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            10 AS month,
            ltv_cumulative_10 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            11 AS month,
            ltv_cumulative_11 AS ltv_cumulative
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            12 AS month,
            ltv_cumulative_12 AS ltv_cumulative
        FROM {input_table}
    )
    SELECT cohort_month, month, ltv_cumulative
    FROM flattened_ltv
    ON CONFLICT (cohort_month, month) DO NOTHING;
    """
    execute_query(
        engine,
        insert_ltv_cumulative_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу ltv_cum_t"
    )
    
def transpon_arppu(
    engine, 
    input_table='cohorts_revenue_arppu_data', 
    output_table='arppu_t'
    ):
    """Преобразует таблицу cohorts_revenue_arppu_data в плоский формат."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        month INT,
        arppu NUMERIC,
        PRIMARY KEY (cohort_month, month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы arppu_t"
    )
    insert_arppu_query = f"""
    INSERT INTO {output_table} (cohort_month, month, arppu)
    WITH flattened_arppu AS (
        SELECT
            cohort_month,
            1 AS month,
            arppu_month_1 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            2 AS month,
            arppu_month_2 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            3 AS month,
            arppu_month_3 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            4 AS month,
            arppu_month_4 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            5 AS month,
            arppu_month_5 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            6 AS month,
            arppu_month_6 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            7 AS month,
            arppu_month_7 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            8 AS month,
            arppu_month_8 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            9 AS month,
            arppu_month_9 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            10 AS month,
            arppu_month_10 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            11 AS month,
            arppu_month_11 AS arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            12 AS month,
            arppu_month_12 AS arppu
        FROM {input_table}
    )
    SELECT cohort_month, month, arppu
    FROM flattened_arppu
    ON CONFLICT (cohort_month, month) DO NOTHING;
    """
    execute_query(
        engine,
        insert_arppu_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу arppu_t"
    )
    
def transpon_cumulative_arppu(
    engine, 
    input_table='arppu_cumulative', 
    output_table='arppu_cum_t'):
    """Преобразует таблицу arppu_cumulative в плоский формат."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        month INT,
        cumulative_arppu NUMERIC,
        PRIMARY KEY (cohort_month, month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы arppu_cum_t"
    )
    
    insert_cumulative_arppu_query = f"""
    INSERT INTO {output_table} (cohort_month, month, cumulative_arppu)
    WITH flattened_cumulative_arppu AS (
        SELECT
            cohort_month,
            1 AS month,
            cumulative_arppu_1 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            2 AS month,
            cumulative_arppu_2 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            3 AS month,
            cumulative_arppu_3 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            4 AS month,
            cumulative_arppu_4 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            5 AS month,
            cumulative_arppu_5 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            6 AS month,
            cumulative_arppu_6 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            7 AS month,
            cumulative_arppu_7 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            8 AS month,
            cumulative_arppu_8 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            9 AS month,
            cumulative_arppu_9 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            10 AS month,
            cumulative_arppu_10 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            11 AS month,
            cumulative_arppu_11 AS cumulative_arppu
        FROM {input_table}
        UNION ALL
        SELECT
            cohort_month,
            12 AS month,
            cumulative_arppu_12 AS cumulative_arppu
        FROM {input_table}
    )
    SELECT cohort_month, month, cumulative_arppu
    FROM flattened_cumulative_arppu
    ON CONFLICT (cohort_month, month) DO NOTHING;
    """
    execute_query(
        engine,
        insert_cumulative_arppu_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу arppu_cum_t"
    )
    
def transpon_rr(
    engine, 
    input_table='rr_cohorts', 
    output_table='rr_t'
):
    """Преобразует таблицу rr_cohorts в плоский формат."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        month INT,
        rr NUMERIC,
        PRIMARY KEY (cohort_month, month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы rr_t"
    )

    insert_rr_query = f"""
    INSERT INTO {output_table} (cohort_month, month, rr)
    WITH flattened_rr AS (
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,  -- Преобразование текста в дату
            1 AS month,
            "RR Month 1" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            2 AS month,
            "RR Month 2" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            3 AS month,
            "RR Month 3" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            4 AS month,
            "RR Month 4" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            5 AS month,
            "RR Month 5" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            6 AS month,
            "RR Month 6" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            7 AS month,
            "RR Month 7" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            8 AS month,
            "RR Month 8" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            9 AS month,
            "RR Month 9" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            10 AS month,
            "RR Month 10" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            11 AS month,
            "RR Month 11" AS rr
        FROM {input_table}
        UNION ALL
        SELECT
            TO_DATE(cohort, 'YYYY-MM-DD') AS cohort_month,
            12 AS month,
            "RR Month 12" AS rr
        FROM {input_table}
    )
    SELECT cohort_month, month, rr
    FROM flattened_rr
    ON CONFLICT (cohort_month, month) DO NOTHING;
    """
    execute_query(
        engine,
        insert_rr_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу rr_t"
    )
    
def transpon_ac(
    engine, 
    input_table='ac_cohort', 
    output_table='ac_t'):
    """Преобразует таблицу ac_cohort в плоский формат."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        cohort_month DATE,
        month INT,
        average_check NUMERIC,
        PRIMARY KEY (cohort_month, month)
    );
    """
    execute_query(
        engine, 
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы ac_t"
    )
    
    insert_ac_query = f"""
    INSERT INTO {output_table} (cohort_month, month, average_check)
    WITH flattened_ac AS (
        SELECT
            -- Преобразуем строку в дату, добавляя '-01', если необходимо
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            0 AS month,
            average_check_current_month AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            1 AS month,
            average_check_month_1 AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            2 AS month,
            average_check_month_2 AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            3 AS month,
            average_check_month_3 AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            4 AS month,
            average_check_month_4 AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            5 AS month,
            average_check_month_5 AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            6 AS month,
            average_check_month_6 AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            7 AS month,
            average_check_month_7 AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            8 AS month,
            average_check_month_8 AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            9 AS month,
            average_check_month_9 AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            10 AS month,
            average_check_month_10 AS average_check
        FROM {input_table}
        UNION ALL
        SELECT
            CAST(CONCAT(cohort, '-01') AS DATE) AS cohort_month,
            11 AS month,
            average_check_month_11 AS average_check
        FROM {input_table}
    )
    SELECT cohort_month, month, average_check
    FROM flattened_ac
    ON CONFLICT (cohort_month, month) DO NOTHING;
    """
    
    execute_query(
        engine,
        insert_ac_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу ac_t"
    )
    
def paid_only(
    engine, 
    input_table='orders', 
    output_table='paid_only', 
    date_format='DD.MM.YYYY HH24:MI:SS'
    ):
    """Создает таблицу paid_only."""
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        user_id BIGINT,
        order_id BIGINT,
        order_date TIMESTAMP,
        order_price NUMERIC
    );
    """
    execute_query(
        engine,
        create_table_query,
        f"Таблица {output_table} успешно создана.",
        "Ошибка при создании таблицы paid_only"
    )
    insert_paid_only_query = f"""
    INSERT INTO {output_table} (user_id, order_id, order_date, order_price)
    SELECT
        "OrderCustomerIdsMindboxId" AS user_id,
        "OrderFirstActionIdsMindboxId" AS order_id,
        TO_TIMESTAMP("OrderFirstActionDateTimeUtc", '{date_format}') AS order_date,
        "OrderTotalPrice" AS order_price
    FROM
        {input_table}
    WHERE
        "OrderLineStatusIdsExternalId" = 'Paid';
    """
    execute_query(
        engine,
        insert_paid_only_query,
        f"Данные для таблицы {output_table} успешно добавлены.",
        "Ошибка при добавлении данных в таблицу paid_only"
    )


def cohort_analysis(engine):
    calculate_cohorts_all(engine)
    calculate_cohorts_paid(engine)
    calculate_arppu(engine)
    calculate_arppu_cumulative(engine)
    calculate_ltv_cohorts(engine)
    calculate_cumulative_ltv(engine)
    calculate_rr(engine)
    calculate_ac(engine)
    paid_only(engine)

def transpon(engine):
    transpon_ltv(engine)
    transpon_revenue(engine)
    transpon_cumulative_ltv(engine)
    transpon_arppu(engine)
    transpon_cumulative_arppu(engine)
    transpon_rr(engine)
    transpon_ac(engine)