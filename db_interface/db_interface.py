import psycopg2

class database_postgres:
    def __init__(self,name:str,host:str,user:str,password:str,port:int=5432):
        self.name=name
        self.host=host
        self.user=user
        self.password=password
        self.port=port

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
        print(cur.execute(statement))

    def create_table(self,table_name:str):
        pass

if __name__=="__main__":
    db=database_postgres("telegram_scraper","localhost","admin","admin")
    db.execute_statement("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")