import psycopg2
import pandas as pd
from sqlalchemy import create_engine

class database_postgres:
    def __init__(self,name:str,host:str,user:str,password:str,port:int=5432):
        self.name=name
        self.host=host
        self.user=user
        self.password=password
        self.port=port

    def get_engine(self):
        engine = create_engine('postgresql://postgres:postgres@host:port/dbname?gssencmode=disable')

    def get_connection(self):
        return psycopg2.connect(
                database=self.name,
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
            )

    def execute_statement(self,statement:str):
        conn=self.get_connection()
        cur=conn.cursor()
        cur.execute(statement)
        conn.commit()

    def create_table(self,table_name:str,columns:dict[str,str]):
        create_statement=f"CREATE TABLE {table_name} "
        columns_statement="( "
        for col in columns.keys():
            columns_statement+=f"{col} {columns[col]},"
        columns_statement=columns_statement.removesuffix(",")+");"
        self.execute_statement(create_statement+columns_statement)

if __name__=="__main__":
    db=database_postgres("telegram_scraper","127.0.0.1","admin","admin")
    #db.execute_statement("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
    db.create_table(table_name="channels_dumped",columns={"id":"integer","name":"varchar"})