import sqlite3


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
    
    
class Database(object):
    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, timeout=30, check_same_thread=False, isolation_level=None)
        self.connection.row_factory = dict_factory
        self.cursor = self.connection.cursor()
        
    def query(self, query, values=None, fetch_one=False):
        if values is None:
            if fetch_one:
                return self.cursor.execute(query).fetchone()
            else:
                return self.cursor.execute(query).fetchall()
        else:
            return self.cursor.execute(query, values)
    
    def commit(self, query=None):
        """ automatically commit or rollback on exception """
        if query is not None:
            with self.connection:
                return self.cursor.execute(query)
        else:
            self.connection.commit()
                
    def __del__(self):
        if self.connection:
            self.connection.close()
