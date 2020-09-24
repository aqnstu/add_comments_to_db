import db_config as db
import os
import sqlalchemy


def get_left_two_cols_from_csv(path):
    with open(path,'r') as csv_file:
        lines = csv_file.readlines()
    
    first_col_lst = []
    second_col_lst = []
    for line in lines[1:]:
        line_elems_lst = line.split(',')
        first_col_lst.append(line_elems_lst[0])
        second_col_lst.append(line_elems_lst[1].rstrip())
    
    return first_col_lst, second_col_lst
    

def main():
    # создаем engine для взаимодействия с БД 
    engine = sqlalchemy.create_engine(
        f'{db.driver}://{db.username}:{db.password}@{db.hostname}:{db.port}/{db.db_name}',
        echo=True
    )
    
    # схема loaded
    loaded_names_lst, loaded_comments_lst = get_left_two_cols_from_csv(os.path.join('csv', 'loaded.csv'))
    loaded_schema = 'loaded'
    
    with engine.connect() as conn:
        for i in range(len(loaded_names_lst)):
            try:
                with conn.begin():
                    engine.execution_options(autocommit=True).execute(f"COMMENT ON TABLE {loaded_schema}.{loaded_names_lst[i]} is \'{loaded_comments_lst[i]}\'")
            except:
                pass
    
    # схема prognoz
    prognoz_names_lst, prognoz_comments_lst = get_left_two_cols_from_csv(os.path.join('csv', 'prognoz.csv'))
    prognoz_schema = 'prognoz'
    with engine.connect() as conn:
        for i in range(len(prognoz_names_lst)):
            try:
                with conn.begin():
                    engine.execution_options(autocommit=True).execute(f"COMMENT ON TABLE {prognoz_schema}.{prognoz_names_lst[i]} is \'{prognoz_comments_lst[i]}\'")
            except:
                pass
    
    # схема data
    data_names_lst, data_comments_lst = get_left_two_cols_from_csv(os.path.join('csv', 'data.csv'))
    data_schema = 'data'
    
    with engine.connect() as conn:
        for i in range(len(data_names_lst)):
            try:
                with conn.begin():
                    if '*' not in data_names_lst[i] or '*' not in data_names_lst[i]:
                        engine.execution_options(autocommit=True).execute(f"COMMENT ON TABLE {data_schema}.{data_names_lst[i]} is \'{data_comments_lst[i]}\'")
            except:
                pass
        
if __name__ == "__main__":
    main()