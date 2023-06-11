from utils import create_database, save_data_to_database
from config import config


def main():
    employer_ids = [3529, 1740, 1057, 2180, 84585, 78638, 4023, 15478, 39305, 3809]  # список айдишек работодателей
    db_name = 'employers_and_vacancies'
    params = config()

    create_database(db_name, params)
    save_data_to_database(db_name, params, employer_ids)


if __name__ == '__main__':
    main()

