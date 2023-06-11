import psycopg2
import requests


def create_database(db_name, params_db):
    """
    Создание базы данных и таблиц для сохранения данных о работодателей и вакансиях
    """
    conn = psycopg2.connect(dbname='postgres', **params_db)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f'DROP DATABASE IF EXISTS {db_name}')
    cur.execute(f'CREATE DATABASE {db_name}')

    cur.close()
    conn.close()

    with psycopg2.connect(dbname=db_name, **params_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE employers (
                    employer_id SERIAL PRIMARY KEY,
                    name_company VARCHAR(255),
                    url_company TEXT,
                    number_of_vacancies INT 
                )
                """
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE vacancies (
                    vacancy_id SERIAL PRIMARY KEY,
                    employer_id INT REFERENCES employers(employer_id),
                    name_of_vacancy VARCHAR(255),
                    salary INT,
                    url_vacancy TEXT,
                    city varchar(255),
                    requirements TEXT
                )
                """
            )

    conn.close()


def save_data_to_database(database_name, params_db, employers_ids):
    """
    Сохранение данных о компаниях и вакансиях в базу данных
    """
    conn = psycopg2.connect(dbname=database_name, **params_db)

    with conn.cursor() as cur:
        for employer_id in employers_ids:
            url = 'https://api.hh.ru/vacancies'
            params = {'employer_id': employer_id, 'per_page': 3}

            response = requests.get(url, params=params)
            vacancies = response.json()

            employer_data = [vacancies["items"][0]["employer"]["name"], f"https://hh.ru/employer/{employer_id}",
                             vacancies["found"]]

            cur.execute(
                """
                INSERT INTO employers (name_company, url_company, number_of_vacancies)
                VALUES (%s, %s, %s)
                RETURNING employer_id
                """,
                employer_data
            )

            employer_id = cur.fetchone()[0]
            vacancies_data = vacancies["items"]

            for vacancy in vacancies_data:

                if vacancy.get('salary'):
                    if vacancy['salary'].get('from'):
                        salary = vacancy['salary']['from']
                    elif not vacancy['salary'].get('from') and vacancy['salary'].get('to'):
                        salary = vacancy['salary']['to']
                else:
                    salary = 0

                vacancy_data = [
                    employer_id,
                    vacancy['name'],
                    salary,
                    vacancy['alternate_url'],
                    vacancy.get('address') and vacancy['address'].get('city', 'Город не указан') or 'Город не указан',
                    vacancy['snippet']['requirement']
                ]

                cur.execute(
                    """
                    INSERT INTO vacancies (employer_id, name_of_vacancy, salary, url_vacancy, city, requirements)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    vacancy_data
                )

    conn.commit()
    conn.close()
