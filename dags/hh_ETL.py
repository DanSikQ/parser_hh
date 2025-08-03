from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from psycopg2.extras import execute_batch
import datetime
import pendulum
import sys
import os
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath('/opt/airflow'))
from scripts import auto_parser

POSTGRES_CONN_ID = 'my_db'

def create_employers_table():
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                  CREATE TABLE IF NOT EXISTS employers (
                      id_employer INTEGER PRIMARY KEY,
                      name_employer VARCHAR(200),
                      accredited_it_employer TEXT,
                      trusted BOOLEAN);
                  """)
                conn.commit()
                print("Таблица 'employers' создана или уже существует.")
    except Exception as e:
        print(f"Ошибка при создании таблицы 'employers': {e}")
        raise


def create_skills_table():
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                  CREATE TABLE IF NOT EXISTS skills (
                      id INTEGER REFERENCES vacancy(id), 
                      skills_in_desc TEXT
                      );
                  """)
                conn.commit()
                print("Таблица 'skills' создана или уже существует.")
    except Exception as e:
        print(f"Ошибка при создании таблицы 'skills': {e}")
        raise

def create_vacancy_table():
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                  CREATE TABLE IF NOT EXISTS vacancy (
                    id INTEGER PRIMARY KEY,
                    premium BOOLEAN,
                    name TEXT,
                    has_test BOOLEAN,
                    response_letter_required BOOLEAN,
                    published_at TIMESTAMP WITH TIME ZONE,
                    apply_alternate_url TEXT,
                    alternate_url TEXT,
                    from_salary NUMERIC(10,2),
                    to_salary NUMERIC(10,2),
                    currency VARCHAR(20),
                    gross TEXT,
                    city TEXT,
                    street TEXT,
                    building TEXT,
                    lat NUMERIC(10,6),
                    lng NUMERIC(10,6),
                    raw TEXT,
                    metro_station_name TEXT,
                    metro_line_name TEXT,
                    id_employer INTEGER REFERENCES employers(id_employer),
                    requirement TEXT,
                    responsibility TEXT,
                    schedules TEXT,
                    format_works TEXT, 
                    hours_working TEXT,
                    schedule_work_by_days TEXT,
                    roles_professional TEXT,
                    experience_ TEXT,
                    employment_ TEXT, 
                    total_salary NUMERIC(10,2),
                    description TEXT, 
                    key_skills_under_desc TEXT,
                    skills_in_desc TEXT,
                    is_intern BOOLEAN   
              );
                  """)
                conn.commit()
                print("Таблица 'skills' создана или уже существует.")
    except Exception as e:
        print(f"Ошибка при создании таблицы 'skills': {e}")
        raise


def load_employers_data(employer_df):
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                columns = ', '.join(employer_df.columns)
                placeholders = ', '.join(['%s'] * len(employer_df.columns))
                upsert_query = (f"INSERT INTO employers ({columns}) "
                                f"VALUES ({placeholders}) "
                                f"ON CONFLICT (id_employer) DO NOTHING")

                for _, row in employer_df.iterrows():
                    try:
                        cursor.execute(f"INSERT INTO employers ({columns}) "
                                f"VALUES ({placeholders}) "
                                f"ON CONFLICT (id_employer) DO NOTHING", tuple(row))
                    except Exception as e:
                        print(f"Error inserting row into 'employers': {e}")
                        print(f"Row data: {row.to_dict()}")
                        continue
                conn.commit()
                print(f"Data loaded into 'employers' ({len(employer_df)} rows attempted).")
    except Exception as e:
        print(f"Error loading data into 'employers': {e}")
        raise


def load_skills_data(vacancy_skills_df):
    print("Loading skills data...")
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                if vacancy_skills_df.empty:
                    print("No skills data to load.")
                    return

                columns = ', '.join(vacancy_skills_df.columns)
                placeholders = ', '.join(['%s'] * len(vacancy_skills_df.columns))

                for _, row in vacancy_skills_df.iterrows():
                    cursor.execute("DELETE FROM skills WHERE id = %s", (row['id'],))

                for _, row in vacancy_skills_df.iterrows():
                    cursor.execute(f"INSERT INTO skills ({columns}) "
                                       f"VALUES ({placeholders})", tuple(row))
                conn.commit()

                print(f"Data loaded into 'skills' ({len(vacancy_skills_df)} rows).")
    except Exception as e:
        print(f"Error loading data into 'skills': {e}")
        raise


def load_vacancy_data(vacancy_df):
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                columns = ', '.join(vacancy_df.columns)
                placeholders = ', '.join(['%s'] * len(vacancy_df.columns))

                data_to_insert = []
                for _, row in vacancy_df.iterrows():
                    row_data = []
                    for value in row:
                        if isinstance(value, (list, np.ndarray)):
                            row_data.append(str(value))
                        elif hasattr(value, 'item'):
                            row_data.append(value.item())
                        elif pd.isna(value):
                            row_data.append(None)
                        else:
                            row_data.append(value)
                    data_to_insert.append(tuple(row_data))


                execute_batch(
                    cursor,
                    f"INSERT INTO vacancy ({columns}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING",
                    data_to_insert
                )

                print(f"Успешно загружено {len(data_to_insert)} записей в vacancy")
    except Exception as e:
        print(f"Ошибка при загрузке данных. Пример строки: {vacancy_df.iloc[0].to_dict()}")
        print("Типы данных в строке:", {k: type(v) for k, v in vacancy_df.iloc[0].items()})
        raise

def unload_from_db():
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with postgres_hook.get_conn() as conn:
        vacancy_info = pd.read_sql_query('''SELECT vacancy.*, emp.name_employer, emp.accredited_it_employer, emp.trusted  FROM vacancy
                 LEFT JOIN employers emp on vacancy.id_employer = emp.id_employer
                 WHERE published_at BETWEEN NOW() - INTERVAL '7 days' and NOW()''', conn)

        vacancy_info.drop(columns=['schedule_work_by_days'], inplace=True)
        vacancy_info.to_csv('/opt/airflow/data/vacancy_info.csv', index=False)

        skills_info = pd.read_sql_query('''SELECT skills.* FROM skills
                    LEFT JOIN vacancy on skills.id = vacancy.id
                    WHERE published_at BETWEEN NOW() - INTERVAL '7 days' and NOW()''', conn)
        skills_info = skills_info[skills_info['skills_in_desc'].notna()]
        skills_info.to_csv('/opt/airflow/data/skills_info.csv', index=False)

with DAG(
    'create_simple_table',
    default_args={
    'owner': 'airflow',
    'start_date': pendulum.datetime(2015, 12, 1, tz="UTC"),
},
    schedule = '5 10 * * *',
    catchup=False,
    tags=['hh_load'],
    max_active_runs=1,
) as dag:

    # a.  Загрузка основных данных о вакансиях
    get_hh_main_data_task = PythonOperator(task_id='get_hh_main_data',
                                           python_callable = auto_parser.init_hh_main_data,
                                           )

    # b.  Загрузка текста вакансий
    load_hh_vac_data_task = PythonOperator(
        task_id='load_hh_vac_data',
        python_callable = auto_parser.load_info_vac,
        op_kwargs={'info_vac_v1': get_hh_main_data_task.output},
    )

    # c.  Объединение и обработка данных
    create_hh_vac_data_merge_task = PythonOperator(
        task_id='create_hh_vac_data_merge',
        python_callable= auto_parser.create_hh_vac_data_merge,
        op_kwargs={'hh_main_data': get_hh_main_data_task.output, 'hh_vac_data': load_hh_vac_data_task.output},
    )

    # d. Создание DataFrame для skills
    create_vacancy_skills_task = PythonOperator(
        task_id='create_vacancy_skills',
        python_callable= auto_parser.load_skills,
        op_kwargs={'hh_vac_data_merge': create_hh_vac_data_merge_task.output},
    )

    # e. Создание DataFrame для employer
    create_employer_task = PythonOperator(
        task_id='create_employer',
        python_callable=auto_parser.load_employers,
        op_kwargs={'hh_vac_data_merge': create_hh_vac_data_merge_task.output},
    )
    # f. Создание DataFrame для vacancy
    create_vacancy_task = PythonOperator(
        task_id='create_vacancy',
        python_callable=auto_parser.load_vacancy,
        op_kwargs={'hh_vac_data_merge': create_hh_vac_data_merge_task.output},
    )
# ------------------------------------------------------------

# Загрузка данных в базу данных
    create_vacancy_table_task = PythonOperator(
        task_id='create_vacancy_table',
        python_callable=create_vacancy_table,
    )

    create_employers_table_task = PythonOperator(
        task_id='create_employers_table',
        python_callable=create_employers_table,
    )

    create_skills_table_task = PythonOperator(
        task_id='create_skills_table',
        python_callable=create_skills_table,
    )


    load_vacancy_data_task = PythonOperator(
        task_id='load_vacancy_data',
        python_callable=load_vacancy_data,
        op_kwargs={'vacancy_df': create_vacancy_task.output},
    )

    load_employers_data_task = PythonOperator(
        task_id='load_employers_data',
        python_callable=load_employers_data,
        op_kwargs={'employer_df': create_employer_task.output},
    )

    load_skills_data_task = PythonOperator(
        task_id='load_skills_data',
        python_callable=load_skills_data,
        op_kwargs={'vacancy_skills_df': create_vacancy_skills_task.output},
    )

    unload_for_bi = PythonOperator(
        task_id='unload_for_bi',
        python_callable = unload_from_db
    )

    get_hh_main_data_task >> load_hh_vac_data_task >> create_hh_vac_data_merge_task >> create_vacancy_skills_task >> create_employer_task >> create_vacancy_task
    create_employers_table_task >> create_vacancy_table_task  >> create_skills_table_task >> load_employers_data_task >> load_vacancy_data_task >>  load_skills_data_task >> unload_for_bi