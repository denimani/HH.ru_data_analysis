import psycopg2
from config import config


class DBManager:
    """
    Класс для подключения БД Postgres
    """

    def __init__(self, db_name, params_db):
        self.cur = psycopg2.connect(dbname=db_name, **params_db).cursor()

    def get_companies_and_vacancies_count(self):
        """
        Метод для получения списка всех компаний и количество вакансий у каждой компании
        """
        self.cur.execute(
            """
            SELECT name_company, number_of_vacancies FROM employers
            """
        )

        rows = self.cur.fetchall()
        results = [{'Компания': row[0], 'Зарплата': row[1]} for row in rows]

        return results

    def get_all_vacancies(self):
        """
        Метод для получения списка всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию.
        """
        self.cur.execute(
            """
            SELECT name_company, name_of_vacancy, salary, url_vacancy 
            FROM vacancies
            JOIN employers USING(employer_id)
            """
        )

        rows = self.cur.fetchall()
        results = [{'Компания': row[0], 'Вакансия': row[1], 'Зарплата': row[2], 'Ссылка на вакансию': row[3]} for
                   row in rows]

        return results

    def get_avg_salary(self):
        """
        Метод для получения средней зарплаты
        """
        self.cur.execute(
            """
            SELECT ROUND(AVG(salary), 2)
            FROM vacancies
            """
        )

        result = self.cur.fetchall()
        return f'Средняя зарплата всех вакансий - {result[0][0]} руб.'

    def get_vacancies_with_higher_salary(self):
        """
        Метод для получения списка всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        self.cur.execute(
            f"""
                SELECT name_of_vacancy
                FROM vacancies
                WHERE salary > {self.get_avg_salary().split(' ')[5]}
                """
        )

        rows = self.cur.fetchall()
        results = [{'Компания': row[0]} for row in rows]

        return results

    def get_vacancies_with_keyword(self, key_word):
        """
        Метод для получения списка всех вакансий, в названии которых содержатся переданные в метод слова "key_word"
        """
        self.cur.execute(
            f"""
            SELECT name_of_vacancy
            FROM vacancies
            WHERE name_of_vacancy LIKE '%{key_word.lower()}%' OR name_of_vacancy LIKE '%{key_word.capitalize()}%'
            """
        )

        rows = self.cur.fetchall()
        results = [{'Компания': row[0]} for row in rows]

        return results
