import psycopg2
import pandas as pd

class Connection:

    def __init__(self, host='localhost', port='5432', user='', password='', database='prisms'):
        try:
            self._conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
            self.sql = ''
        except:
            print("Database connection failed!")
            exit(1)

    def read(self, table_name, column_set, request_condition=''):
        sql = 'select {columns} from {table} {where}'\
            .format(columns=','.join(column_set), table=table_name, where=request_condition)
        res = self.execute_wi_return(sql)
        return res

    def read_as_dataframe(self,  table_name, column_set, request_condition=''):
        sql = 'select {columns} from {table} {where}'\
            .format(columns=','.join(column_set), table=table_name, where=request_condition)
        df=pd.read_sql(sql, con=self._conn)
        return df


    def get_conn(self):
        return self._conn

    def execute_wi_return(self, sql):
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            res = cur.fetchall()
            cur.close()
            return res
        except ConnectionError:
            print('SQL {} execution Fails.'.format(sql))
            exit(1)
        finally:
            cur.close()

    def execute_wo_return(self, sql):
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            self._conn.commit()
            cur.close()
        except ConnectionError:
            print('SQL {} execution Fails.'.format(sql))
            exit(1)
        finally:
            cur.close()

    def close_conn(self):
        self._conn.close()



