import psycopg2
import pandas as pd
from sqlalchemy import create_engine
columns={
    "_":"varchar",
    "id":"integer",
    "peer_id":"varchar",
    "date":"timestamp",
    "message":"varchar",
    "out":"bool",
    "mentioned":"bool",
    "media_unread":"bool",
    "silent":"bool",
    "post":"bool",
    "from_scheduled":"varchar",
    "legacy":"bool",
    "edit_hide":"varchar",
    "pinned":"varchar",
    "noforwards":"varchar",
    "from_id":"varchar",
    "fwd_from":"varchar",
    "via_bot_id":"varchar",
    "reply_to":"varchar",
    "media":"varchar",
    "reply_markup":"varchar",
    "entities":"varchar",
    "views":"float",
    "forwards":"float",
    "replies":"float",
    "edit_date":"timestamp",
    "post_author":"varchar",
    "grouped_id":"float",
    "reactions":"varchar",
    "restriction_reason":"varchar",
    "ttl_period":"varchar",
    "action":"varchar",
}

class database_postgres:
    def __init__(self,name:str,host:str,user:str,password:str,port:int=5432):
        self.name=name
        self.host=host
        self.user=user
        self.password=password
        self.port=port

    def get_engine(self):
        statement=(
            f"postgresql+psycopg2://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.name}"
            "?gssencmode=disable"
        )
        db=create_engine(statement)
        conn=db.connect()
        return conn

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

    def create_telegram_table(self,name:str):
        self.create_table(name,columns=columns)

if __name__=="__main__":
    db=database_postgres("telegram_scraper","127.0.0.1","admin","admin")
    #db.execute_statement("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
    db.create_table(table_name="dumped_channels",columns={"id":"integer","name":"varchar","datetime":"timestamp"})