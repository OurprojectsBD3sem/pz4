import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import pandas as pd

# Формат подключения: mysql+pymysql://USER:PASSWORD@HOST:PORT/DBNAME
DB_USER = "j30084097"
DB_PASSWORD = "7f9vGAxSu"
DB_HOST = "mysql.65e3ab49565f.hosting.myjino.ru"
DB_PORT = "3306"
DB_NAME = "j30084097_bd_pz4_firfarov_kasatkin_2110"

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


engine = create_engine(DATABASE_URL, echo=True)

SessionLocal = sessionmaker(bind=engine)

# Проверка подключения
with engine.connect() as conn:
    result = conn.execute(text("SELECT NOW();"))
    print

# Create
def create_table_from_df(df, table_name, engine, primary_key=None, if_not_exists=True):
    type_map = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'object': 'TEXT'
    }
    cols = []
    for col, dtype in df.dtypes.items():
        col_def = f"`{col}` {type_map.get(str(dtype), 'TEXT')}"
        if primary_key and col == primary_key:
            col_def += " PRIMARY KEY"
        cols.append(col_def)
    clause = "IF NOT EXISTS " if if_not_exists else ""
    ddl = f"CREATE TABLE {clause}`{table_name}` ({','.join(cols)});"
    with engine.begin() as conn:
        conn.execute(text(ddl))


# SELECT
def select_all(table_name, engine, limit=10):
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {table_name} LIMIT {limit}"), conn)
        return df

# INSERT (одна строка/словарь)
def insert_row(table_name, data_dict, engine):
    cols = ",".join(data_dict.keys())
    placeholders = ",".join([f":{c}" for c in data_dict.keys()])
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    with engine.begin() as conn:
        conn.execute(text(sql), data_dict)

# UPDATE (по условиям)
def update_row(table_name, set_dict, where_dict, engine):
    set_clause = ", ".join([f"{k}=:{k}" for k in set_dict.keys()])
    where_clause = " AND ".join([f"{k}=:{k}_w" for k in where_dict.keys()])
    params = {**set_dict, **{k + "_w": v for k, v in where_dict.items()}}
    sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
    with engine.begin() as conn:
        conn.execute(text(sql), params)

# DELETE (по условиям)
def delete_row(table_name, where_dict, engine):
    where_clause = " AND ".join([f"{k}=:{k}" for k in where_dict.keys()])
    sql = f"DELETE FROM {table_name} WHERE {where_clause};"
    with engine.begin() as conn:
        conn.execute(text(sql), where_dict)

# DROP таблицу
def drop_table(table_name, engine):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))

# Экспорт в csv
def export_table_to_csv(table_name, filename, engine):
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)
    df.to_csv(filename, index=False)

# CREATE
df = pd.read_csv("/Users/mak/Desktop/Titanic-Dataset.csv")
create_table_from_df(df, "titanic", engine)

# SELECT
print(select_all("titanic", engine, limit=5))

# INSERT
insert_row("titanic", {
    "PassengerId": 1001, "Survived": 0, "Pclass": 2, "Name": "Smith, Mr. John", "Sex": "male",
    "Age": 28, "SibSp": 0, "Parch": 0, "Ticket": "PC 123456", "Fare": 12.34, "Cabin": "", "Embarked": "S"
}, engine)

# UPDATE
update_row("titanic", {"Fare": 55.5}, {"PassengerId": 1001}, engine)

# DELETE
delete_row("titanic", {"PassengerId": 1001}, engine)

# DROP
drop_table("titanic", engine)

# EXPORT
export_table_to_csv("titanic", "titanic_copy.csv", engine)


