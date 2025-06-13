import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import logging
from datetime import datetime
import re

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='data_air_processing.log'
)
logger = logging.getLogger(__name__)





def load_data_to_postgres(df, table_name, connection_params):
    """Загружает DataFrame в PostgreSQL"""
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Создаем таблицу, если она не существует
        create_table_query = sql.SQL("""
         CREATE TABLE IF NOT EXISTS air_pollution (
            id SERIAL PRIMARY KEY,
            Адрес TEXT,
            Время TIME,
            CO NUMERIC,
            NO NUMERIC,
            NO2 NUMERIC,
            SO2 NUMERIC,
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
                row['CO(мг/м3)'],
                row['NO(мг/м3)'],
                row['NO2(мг/м3)'],
                row['SO2(мг/м3)'],
                row['Дата']
            )
            records.append(record)
        
        # Вставляем данные пачками
        insert_query = sql.SQL("""
        INSERT INTO {} (Адрес, Время, CO, NO, NO2, SO2, Дата)
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
        df_CO = pd.read_excel(
            "report_Экология_за час_17.03.2025 00_00_17.03.2025 23_59.xlsx", 
            sheet_name=0
        )

        # Загружаем данные с первого листа (транспортные показатели)
        df_NO = pd.read_excel(
            "report_Экология_за час_17.03.2025 00_00_17.03.2025 23_59.xlsx", 
            sheet_name=1
        )

        # Загружаем данные с первого листа (транспортные показатели)
        df_NO2 = pd.read_excel(
            "report_Экология_за час_17.03.2025 00_00_17.03.2025 23_59.xlsx", 
            sheet_name=2
        )
        # Загружаем данные с первого листа (транспортные показатели)
        df_SO2 = pd.read_excel(
            "report_Экология_за час_17.03.2025 00_00_17.03.2025 23_59.xlsx", 
            sheet_name=3
        )

        # Объединение данных
        df_merged = df_CO.join(df_NO["NO(мг/м3)"]).join(df_NO2["NO2(мг/м3)"]).join(df_SO2["SO2(мг/м3)"])

        # Сохранение в CSV
        df_merged.to_csv("air.csv", index=False, encoding="utf-8")
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
        
        load_data_to_postgres(df_merged, "air_pollution", db_params)
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {str(e)}")
        return False

if __name__ == "__main__":
    if process_excel_to_postgres():
        print("Обработка данных успешно завершена!")
    else:
        print("Произошла ошибка при обработке данных. Проверьте лог-файл.")