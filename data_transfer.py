import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='data_processing.log'
)
logger = logging.getLogger(__name__)

def load_data_to_postgres(df, table_name, connection_params):
    """Загружает DataFrame в PostgreSQL"""
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Создаем таблицу, если она не существует
        create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            id SERIAL PRIMARY KEY,
            Адрес TEXT,
            Время TIME,
            Скорость NUMERIC,
            Поток NUMERIC,
            Широта NUMERIC,
            Долгота NUMERIC,
            Дата DATE
        )
        """).format(sql.Identifier(table_name))
        
        cursor.execute(create_table_query)
        conn.commit()
        
        # Подготавливаем данные для вставки
        records = []
        for _, row in df.iterrows():
            record = (
                row['Адрес'],
                row['Время'],
                row['Скорость'],
                row['Поток'],
                row['Широта'],
                row['Долгота'],
                row['Дата']
            )
            records.append(record)
        
        # Вставляем данные пачками
        insert_query = sql.SQL("""
        INSERT INTO {} (Адрес, Время, Скорость, Поток, Широта, Долгота, Дата)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """).format(sql.Identifier(table_name))
        
        execute_batch(cursor, insert_query, records)
        conn.commit()
        
        logger.info(f"Успешно загружено {len(df)} записей в таблицу {table_name}")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке в PostgreSQL: {str(e)}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def process_excel_to_postgres():
    """Основная функция обработки данных"""
    try:
        # Загрузка данных из Excel
        logger.info("Начало обработки файла Excel")
        
        # Загружаем данные с первого листа (транспортные показатели)
        df_metrics = pd.read_excel(
            "Транспортные показатели_17.03.2025 00_00_17.03.2025 23_59.xlsx", 
            sheet_name=0
        ).rename(columns={
            "Средняя скорость, км/ч (за период)": "Скорость",
            "Интенсивность, авто (за период)": "Поток"
        })

        # Загружаем данные со второго листа (адреса и координаты)
        df_coords = pd.read_excel(
            "Транспортные показатели_17.03.2025 00_00_17.03.2025 23_59.xlsx", 
            sheet_name=1,
            usecols=["Адресная привязка", "Долгота", "Широта"]
        ).rename(columns={"Адресная привязка": "Адрес"})

        # Объединение данных
        df_merged = pd.merge(
            df_metrics,
            df_coords,
            on="Адрес",
            how="left"
        )

        # Очистка данных
        df_merged = df_merged[
            (df_merged["Скорость"] > 0) & 
            (df_merged["Поток"] > 0) &
            (df_merged["Широта"].notna()) &
            (df_merged["Долгота"].notna())
        ]

        # Сохранение в CSV
        df_merged.to_csv("output4.csv", index=False, encoding="utf-8")
        logger.info("Данные успешно сохранены в CSV файл")

        # Параметры подключения к PostgreSQL
        db_params = {
            "dbname": "Transport",
            "user": "postgres",
            "password": "Qwerty123",
            "host": "localhost",
            "port": "5432"
        }

        # Загрузка в PostgreSQL
        load_data_to_postgres(df_merged, "transport_metrics", db_params)
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {str(e)}")
        return False

if __name__ == "__main__":
    if process_excel_to_postgres():
        print("Обработка данных успешно завершена!")
    else:
        print("Произошла ошибка при обработке данных. Проверьте лог-файл.")