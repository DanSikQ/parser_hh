import requests
import pandas as pd
import json
import time
import re
import numpy as np
import spacy
import psycopg2

def init_hh_main_data():

    info_vac_v1 = pd.DataFrame()
    # Выгрузка основной информации по вакансиям
    for i in range(0, 1):
        params = {
            'text': '!(аналитик данных OR data analyst OR бизнес-аналитик OR BI-аналитик or data engineer)',
            # Текст фильтра
            'area': 1,  # Поиск ощуществляется по вакансиям города Москва
            'page': i,  # Индекс страницы поиска на HH
            'per_page': 10  # Кол-во вакансий на 1 странице
        }

        req = requests.get('https://api.hh.ru/vacancies', params)  # Посылаем запрос к API
        data = req.content.decode()  # Декодируем его ответ, чтобы Кириллица отображалась корректно
        time.sleep(0.5)
        req.close()
        json_data = json.loads(data)
        new_data = pd.DataFrame(json_data['items']).reset_index(drop=True)
        info_vac_v1 = pd.concat([info_vac_v1, new_data], ignore_index=True)

    # Обработка данных
    salary = pd.json_normalize(info_vac_v1['salary'])

    address = pd.json_normalize(info_vac_v1['address'])
    address = address.drop(columns=['id'])

    employer = pd.json_normalize(info_vac_v1['employer'])
    employer_to_drop = ['logo_urls.90', 'logo_urls.240', 'url', 'alternate_url', 'vacancies_url', 'logo_urls.original',
                        'logo_urls']
    employer_exist_to_drop = [col for col in employer_to_drop if col in employer.columns]
    employer = employer.drop(columns=employer_exist_to_drop)
    employer = employer.rename(columns={'id': 'id_employer', "name": "name_employer"})

    snippet = pd.json_normalize(info_vac_v1['snippet'])

    schedule = pd.json_normalize(info_vac_v1['schedule'])
    schedule.drop(columns=['id'], inplace=True)
    schedule.rename(columns={'name': 'schedules'}, inplace=True)

    def sep_list_columns(list):
        length = len(list)
        if length >= 1:
            return list[0]

    work_format = pd.json_normalize(info_vac_v1['work_format'].apply(sep_list_columns))
    work_format.rename(columns={'name': 'format_works'}, inplace=True)

    working_hours = pd.json_normalize(info_vac_v1['working_hours'].apply(sep_list_columns))
    working_hours.rename(columns={'name': 'hours_working'}, inplace=True)

    work_schedule_by_days = pd.json_normalize(info_vac_v1['work_schedule_by_days'].apply(sep_list_columns))
    work_schedule_by_days.rename(columns={'name': 'schedule_work_by_days'}, inplace=True)

    professional_roles = pd.json_normalize(info_vac_v1['professional_roles'].apply(sep_list_columns))
    professional_roles.rename(columns={'name': 'roles_professional'}, inplace=True)

    experience = pd.json_normalize(info_vac_v1['experience'])
    experience = experience.drop(columns=['id'])
    experience = experience.rename(columns={'name': 'experience_'})

    employment = pd.json_normalize(info_vac_v1['employment'])
    employment = employment.drop(columns=['id'])
    employment = employment.rename(columns={'name': 'employment_'})

    info_vac_v1 = pd.concat([info_vac_v1, salary, address, employer, snippet, schedule, work_format['format_works'],
                             working_hours['hours_working'], work_schedule_by_days['schedule_work_by_days'],
                             professional_roles['roles_professional'], experience['experience_'],
                             employment['employment_']], axis=1)
    columns_to_drop = [
        'area', 'salary', 'employment_form', 'salary_range', 'type', 'address',
        'employer', 'contacts', 'snippet', 'schedule', 'work_format',
        'employment_form', 'working_hours', 'work_schedule_by_days',
        'professional_roles', 'experience', 'employment', 'metro_stations',
        'branding', 'department', 'description', 'insider_interview',
        'response_url', 'sort_point_distance', 'adv_response_url', 'adv_context',
        'metro', 'relations', 'working_days', 'working_time_intervals',
        'working_time_modes', 'fly_in_fly_out_duration', 'night_shifts',
        'is_adv_vacancy', 'archived', 'show_logo_in_search', 'show_contacts',
        'brand_snippet', 'created_at', 'url', 'accept_temporary',
        'accept_incomplete_resumes', 'internship', 'metro.station_id',
        'metro.line_id', 'metro.lat', 'metro.lng'
    ]
    existing_columns_to_drop = [col for col in columns_to_drop if col in info_vac_v1.columns]
    info_vac_v1 = info_vac_v1.drop(columns=existing_columns_to_drop)
    exchange_rates = {
        'USD': 80.0,  # 1 USD = 80 RUB
        'EUR': 90.0,  # 1 EUR = 90 RUB
        'RUR': 1.0  # 1 RUB = 1 RUB
    }
    # Обработка зарплаты
    def check_salary(row):
        if pd.isna(row['to']) and pd.isna(row['from']):
            return np.nan
        elif pd.isna(row['to']):
            total_salary = row['from']
        elif pd.isna(row['from']):
            total_salary = row['to']
        else:
            total_salary = (row['from'] + row['to']) / 2
        currency = row['currency']
        if pd.notna(currency):
            total_salary *= exchange_rates.get(currency, 1.0)  # Конвертация в рубли
        if pd.notna(row['gross']) and row['gross'] == True:
            total_salary *= 0.87  # Вычет налогов
        return total_salary
    info_vac_v1['total_salary'] = info_vac_v1.apply(check_salary, axis=1)

    return info_vac_v1


def load_info_vac(info_vac_v1):
    description_info = []
    for id in info_vac_v1['id']:
        try:
            # Делаем запрос к API
            req_vac = requests.get(f'https://api.hh.ru/vacancies/{id}')
            req_vac.raise_for_status()
            data = req_vac.content.decode()
            req_vac.close()

            time.sleep(0.4)  # Задержка между запросами

            data_vac = json.loads(data)

            # Обработка skills с проверкой наличия ключа
            skills = []
            if 'key_skills' in data_vac and isinstance(data_vac['key_skills'], list):
                skills = [i['name'] for i in data_vac['key_skills']]

            description_info.append({
                'vacancy_id': id,
                'description': data_vac.get('description', ''),
                'key_skills_under_desc': skills,
            })
        except Exception as e:
            print(f"Неожиданная ошибка для вакансии {id}: {e}")
            continue

    return description_info

def extract_skills(desc):
    skills_keywords = [
        # Языки программирования
        'SQL', 'Python', 'R', 'Scala', 'Java', 'C#', 'Julia', '1C',

        # BI и визуализация
        'Power BI', 'Tableau', 'Qlik', 'Looker', 'Metabase', 'Redash', 'Superset',
        'Excel', 'Google Sheets', 'Data Studio', 'Plotly', 'Matplotlib', 'Seaborn',

        # Базы данных
        'PostgreSQL', 'MySQL', 'MS SQL', 'Oracle', 'ClickHouse', 'Greenplum',
        'MongoDB', 'Redis', 'Cassandra', 'Snowflake', 'BigQuery', 'Redshift',

        # Big Data
        'Hadoop', 'Spark', 'Hive', 'Kafka', 'Airflow', 'Flink', 'Databricks',

        # Облачные платформы
        'AWS', 'Azure', 'GCP', 'Yandex Cloud', 'IBM Cloud',

        # ETL и обработка данных
        'DWH', 'ETL', 'ELT', 'Data Vault', 'Data Lake', 'Data Mesh',
        'Informatica', 'Talend', 'SSIS', 'Alteryx', 'dbt', 'Apache NiFi',

        # Аналитика
        'Machine Learning', 'ML', 'AI', 'Deep Learning', 'NLP', 'Computer Vision',
        'Statistics', 'A/B тестирование', 'Predictive Modeling', 'Time Series',
        'EDA', 'Feature Engineering', 'MLflow', 'Kubeflow',

        # Управление
        'Scrum', 'Agile', 'Kanban', 'Jira', 'Confluence', 'Git', 'CI/CD',

        # Дополнительные технологии
        'Docker', 'Kubernetes', 'Linux', 'Bash', 'Pandas', 'NumPy', 'SciPy',
        'Scikit-learn', 'TensorFlow', 'PyTorch', 'Keras', 'XGBoost', 'CatBoost',
        'LightGBM', 'OpenCV', 'NLTK', 'spaCy', 'Hugging Face', 'REST', 'API', 'request', 'WebSocket',

        # Математика
        'Математика', 'Статистика', 'Математический анализ', 'Линейная алгебра',
        'Теория Вероятностей', 'Дискретная математика']

    found_skills = []
    desc = str(desc).lower()
    for skill in skills_keywords:
        if re.search(r'\b' + re.escape(skill.lower()) + r'\b', desc):
            found_skills.append(skill)
    return found_skills

nlp = spacy.load("ru_core_news_sm")

def check_intern_nlp(desc):
    doc = nlp(desc.lower())

    internship_lemmas = {
        "интерн", "стажёр",
        "intern", "trainee"
    }
    for token in doc:
        # Проверяем лемму (начальную форму) слова
        if token.lemma_ in internship_lemmas:
            return True

    return False


def main():
    hh_main_data = init_hh_main_data()
    hh_vac_data = load_info_vac(hh_main_data)

    df_hh_vac_data = pd.DataFrame(hh_vac_data)
    hh_vac_data_merge = hh_main_data.merge(df_hh_vac_data, left_on='id', right_on='vacancy_id', how='left').drop(
        columns=['vacancy_id'])

    hh_vac_data_merge.rename(columns={'description_y': 'description',
                                   'key_skills_under_desc_y': 'key_skills_under_desc'}, inplace=True)
    # info_vac_merge.to_csv('info_vac.csv')

    hh_vac_data_merge.columns = map(lambda x: x.replace('.', '_'), hh_vac_data_merge.columns.to_list())

    hh_vac_data_merge['skills_in_desc'] = hh_vac_data_merge['description'].apply(extract_skills)
    hh_vac_data_merge = hh_vac_data_merge[hh_vac_data_merge['id'].notna()]

    hh_vac_data_merge['is_intern'] = hh_vac_data_merge['name'].apply(check_intern_nlp)
    hh_vac_data_merge = hh_vac_data_merge[hh_vac_data_merge['id_employer'].notna()]

    hh_vac_data_merge['published_at'] = pd.to_datetime(hh_vac_data_merge['published_at'], utc=True)
    hh_vac_data_merge.rename(columns={'from': 'from_salary',
                                   'to': 'to_salary'}, inplace=True)

    skills_desc = hh_vac_data_merge['skills_in_desc'].explode('skills_in_desc')
    skills_counts = skills_desc.value_counts()
    vacancy_skills = hh_vac_data_merge[['id', 'skills_in_desc']].explode('skills_in_desc')

    employer = hh_vac_data_merge[['id_employer', 'name_employer', 'accredited_it_employer', 'trusted']].drop_duplicates(
        subset=['id_employer'], keep='first')
    employer = employer[employer['id_employer'].notna()]

    hh_vac_data_merge.drop(columns=['name_employer', 'accredited_it_employer', 'trusted'], inplace=True)
    print(hh_vac_data_merge)

if __name__ == "__main__":
    main()