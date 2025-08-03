# 🚀 Автоматическая выгрузка данных с HeadHunter для анализа вакансий

Этот проект автоматизирует сбор данных о вакансиях с HeadHunter и сохраняет их в базе данных. Он предназначен для аналитиков данных (и других заинтересованных лиц), предоставляя актуальную информацию о рынке труда для анализа навыков, зарплат и трендов.  **Проект также анализирует текст вакансий, выявляя упоминания конкретных навыков и наличие информации о стажировках.**  Проект может быть адаптирован для любых профессий на HeadHunter, автоматизируя сбор данных и экономя время.

----

## 🔧 Требования

*   **Docker:** Версия 20.10 или выше. Убедитесь, что Docker установлен и запущен на вашей системе.
*   **Docker Compose:** Версия 1.29 или выше. Docker Compose должен быть установлен вместе с Docker.
*   **Power BI:** Power BI Desktop или Power BI Pro для просмотра и анализа данных.  Рекомендуется использовать последнюю версию Power BI Desktop для обеспечения совместимости с дашбордом.

---- 

## ⚙️ Инструкция по установке и запуску

1.  **Клонирование репозитория:**

    ```bash
    git clone https://github.com/DanSikQ/parser_hh
    cd parser_hh
    ```

2.  **Запуск Airflow с помощью Docker Compose:**

    Сначала необходимо инициализировать базу данных Airflow:

    ```bash
    docker compose up airflow-init
    ```

    Затем запустите Airflow в фоновом режиме:

    ```bash
    docker-compose up -d --build
    ```

    Этот шаг создаст и запустит контейнеры Docker для Airflow.  Пожалуйста, подождите, пока все контейнеры не будут запущены.


3.  **Настройка Airflow Connection 'my_db':**

    После запуска контейнеров необходимо настроить подключение к базе данных PostgreSQL в Airflow.  Для этого выполните следующие действия:
    
    *   Откройте веб-интерфейс Airflow в браузере по адресу: `http://localhost:8080`
    *   Используйте логин и пароль по умолчанию: `airflow/airflow` (рекомендуется сменить после первого входа!)
    *   Перейдите в раздел "Admin" -> "Connections".
    *   Нажмите кнопку "+" (Create) или "Edit" (если подключение уже существует).
    *   Заполните поля следующим образом:
        *   **Conn Id:** `my_db` (ОБЯЗАТЕЛЬНО!)
        *   **Conn Type:** `Postgres`
        *   **Host:** `postgres` (или `localhost`, если PostgreSQL запущен вне Docker)
        *   **Schema:** `airflow` (или другое имя схемы, если вы его изменили)
        *   **Login:** `airflow` (или имя пользователя PostgreSQL)
        *   **Password:** `airflow` (или пароль пользователя PostgreSQL)
        *   **Port:** `5432` (или порт PostgreSQL)
    *   Нажмите кнопку "Save". ✅

    **Важно:**  Conn Id должен быть именно `my_db.

4.  **Запуск DAG и обновление дашборда Power BI:**

    *   После настройки подключения `my_db` перейдите в раздел "DAGs" в веб-интерфейсе Airflow.
    *   Включите DAG, отвечающий за выгрузку данных с HeadHunter (название DAG - `hh_load`).
    *   DAG будет автоматически запускаться в соответствии с расписанием или вы можете запустить его вручную, нажав кнопку "Trigger DAG".
    *   После успешного выполнения DAG откройте файл дашборда Power BI (`.pbix`).
    *   Нажмите кнопку "Обновить" в Power BI, чтобы обновить данные.

    Теперь в дашборде Power BI будут отображены вакансии, опубликованные за последние 7 дней.

----

## 🗄️ Структура базы данных. 

*   **employers:** Информация о работодателях.
    *   `id_employer`: INTEGER (Первичный ключ)
    *   `name_employer`: VARCHAR(200)
    *   `accredited_it_employer`: TEXT
    *   `trusted`: BOOLEAN


*   **skills:** Навыки, найденные в описаниях вакансий.
    *   `id`: INTEGER (Внешний ключ, ссылается на `vacancy.id`)
    *   `skills_in_desc`: TEXT


*   **vacancy:** Основная информация о вакансиях.
    *   `id`: INTEGER (Первичный ключ)
    *   `premium`: BOOLEAN
    *   `name`: TEXT
    *   `has_test`: BOOLEAN
    *   `response_letter_required`: BOOLEAN
    *   `published_at`: TIMESTAMP WITH TIME ZONE
    *   `apply_alternate_url`: TEXT
    *   `alternate_url`: TEXT
    *   `from_salary`: NUMERIC(10,2)
    *   `to_salary`: NUMERIC(10,2)
    *   `currency`: VARCHAR(20)
    *   `gross`: TEXT
    *   `city`: TEXT
    *   `street`: TEXT
    *   `building`: TEXT
    *   `lat`: NUMERIC(10,6)
    *   `lng`: NUMERIC(10,6)
    *   `raw`: TEXT
    *   `metro_station_name`: TEXT
    *   `metro_line_name`: TEXT
    *   `id_employer`: INTEGER (Внешний ключ, ссылается на `employers.id_employer`)
    *   `requirement`: TEXT
    *   `responsibility`: TEXT
    *   `schedules`: TEXT
    *   `format_works`: TEXT
    *   `hours_working`: TEXT
    *   `schedule_work_by_days`: TEXT
    *   `roles_professional`: TEXT
    *   `experience_`: TEXT
    *   `employment_`: TEXT
    *   `total_salary`: NUMERIC(10,2)
    *   `description`: TEXT
    *   `key_skills_under_desc`: TEXT
    *   `skills_in_desc`: TEXT
    *   `is_intern`: BOOLEAN 


------
## 📂 Структура проекта
```parser_hh/
├── data/                     # Входные данные
│   ├── skills_info.csv       # Информация о скиллах вакансии
│   ├── vacancy_info          # Остальная информация о вакансии
├── dags/                     # DAG с прописанной логикой ETL
│   ├── hh_ETL.py
├── scripts/                  # Основной скрипт
│   ├── auto_parser.py/
├── dahsbord_vacancy.pbix     # Дашборд с основной информацией 
├── .env                      # Переменные окружения
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
