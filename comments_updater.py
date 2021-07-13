"""
Скрипт для добавления комментариев в БД PostgreSQL.
"""
import datetime
import glob
import misc.db_config as db
import os
import sqlalchemy
import sys
import tabulate as t  # ? https://pypi.org/project/tabulate/


def get_all_files_from_dir(dir: str, type: str) -> list:
    """Получить список имен файлов из директории dir с расширением type (без точки)."""
    return glob.glob(os.path.join(dir, f"*.{type}"))


def get_left_two_cols_from_csv(path: str):
    """Получить первых два столбца в виде списков из файла по пути path с расширением .csv."""
    with open(path, "r") as csv_file:
        lines = csv_file.readlines()

    first_col_lst = []
    second_col_lst = []
    for line in lines[1:]:
        line_elems_lst = line.split(",")
        first_col_lst.append(line_elems_lst[0])
        second_col_lst.append(line_elems_lst[1].rstrip())

    return first_col_lst, second_col_lst


def update_comments_in_db(db_name: str = "ias"):
    """Обновить комментарии в БД db_name."""
    engine = sqlalchemy.create_engine(
        f"{db.driver}://{db.username}:{db.password}@{db.hostname}:{db.port}/{db_name}",
        echo=False,
    )

    # получаем пути файлов из директории csv с расширением .csv
    csv_file_paths = get_all_files_from_dir("csv", "csv")
    # получаем отдельно названия файлов (названия схем) без директории и расширения
    csv_file_names = [
        path_name.split(".")[0]
        for path_name in list(map(os.path.basename, csv_file_paths))
    ]

    # временный контейнер
    temp_container = [get_left_two_cols_from_csv(path) for path in csv_file_paths]
    # получаем превые два столюца из всех файлов в виде
    # [ [[<имена_таблиц_из_файла_1>], [<комментарии_таблиц_из_файла_1>]],  [[<имена_таблиц_из_файла_2>], [<комментарии_таблиц_из_файла_2>]], ...]
    container = [[j for j in i] for i in temp_container]

    # все существующие таблицы (только таблицы) в схемах из списка csv_file_names
    existing_full_table_names = []
    with engine.connect() as conn:
        with conn.begin():
            for schema_name in csv_file_names:
                result = engine.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}' AND (table_type = 'BASE TABLE') ORDER BY 1"
                )
                existing_full_table_names.extend(
                    [schema_name + "." + row[0] for row in result]
                )

    # добовление комментариев в таблицы БД
    processed_full_table_names = []
    log = []
    current_time = datetime.datetime.today().strftime("%d-%m-%Y-%H.%M.%S")
    with engine.connect() as conn:
        for schema_name, columns in zip(csv_file_names, container):
            for table_name, comment in zip(columns[0], columns[1]):
                try:
                    with conn.begin():
                        full_table_name = f"{schema_name}.{table_name}"
                        engine.execution_options(autocommit=True).execute(
                            f"COMMENT ON TABLE {full_table_name} is '{comment}'"
                        )
                        processed_full_table_names.append(full_table_name)
                except:
                    log.append([schema_name, table_name, comment, current_time])
    # необработанные таблицы (те, которые не удалось откомментировать)
    log_table = t.tabulate(
        log, headers=["schema", "table", "comment", "date_added"], tablefmt="psql"
    )

    # создаем директорию с логами, если ее не существовало
    if not os.path.isdir("log"):
        os.mkdir("log")
    with open(os.path.join("log", f"{db_name}_{current_time}.txt"), "w+") as f:
        f.write(
            "Таблицы, указанные в файлах директории 'csv', которые не удалось обновить:\n"
        )
        f.write(log_table)
        f.write("\n\n")

    # необработанные таблицы (те, к которым нет комментариев)
    # ищем таблицы, для которых нет комментраиев (совсем) через разность множеств
    remaining_full_table_names = list(
        set(existing_full_table_names) - set(processed_full_table_names)
    )

    remaining_full_table_names_table = t.tabulate(
        [
            table_name.split(".")
            for table_name in sorted(
                [record + "." + current_time for record in remaining_full_table_names]
            )
        ],
        headers=["schema", "table", "date_added"],
        tablefmt="psql",
    )
    with open(os.path.join("log", f"{db_name}_{current_time}.txt"), "a") as f:
        f.write("Таблицы, которых нет в файлах директории 'csv':\n")
        f.write(remaining_full_table_names_table)
        f.write("\n\n")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        try:
            update_comments_in_db(db_name=sys.argv[1])
            print(
                "Программа успешно завершила свою работу.\nЛог доступен в директории log."
            )
        except sqlalchemy.exc.OperationalError:
            print("Некорректное название базы данных, либо она не существует.")
    else:
        "Введите корректные парамерты: python3 comments_updater.py <имя_базы_данных>"
