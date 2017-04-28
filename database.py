#!/usr/bin/env python
#future comptability
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals
from builtins import (ascii, bytes, chr, dict, filter, hex, input,
                      int, map, next, oct, open, pow, range, round,
                      str, super, zip)

import sqlite3
import logging
import sys

logger = logging.getLogger(__name__)

def dict_factory(cursor, row): 
    """
    set cursor.row_factory = dict_factory to get dictionary objects from
    cursor.fetch commands
    """   
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class DbConnection(sqlite3.Connection):
    """
    we extend sqlite3.Connection so we can inherit our extended
    cursor class
    """
    def cursor(self):
        return super(DbConnection, self).cursor(DbCursor)
    
    def close(self):
        logger.info('Database connection closed')
        super().close() 

class DbCursor(sqlite3.Cursor):
    """
    in case we want to extend the cursor class, do it here.
    for instance, i added logging to execute method
    """           
    def execute(self, *params):
        logger.debug('{} {}'.format(params[0], params[1:]))
        try:
            return super(DbCursor, self).execute(*params)
        except Exception as e:
            logger.error('Exception in DbCursor - {}'.format(str(e)))
            raise 
            
class Database(object):
    def __init__(self, filename):
        self._filename = filename
        logger.info('Database connect {}'.format(self._filename))
        self.connection = sqlite3.connect(self._filename, factory=DbConnection)
        self.cursor = self.connection.cursor()
        
        self._table = None  #self.retrieve_table()
        self._columns = None #[(params[1], params[2]) for params in self.retrieve_columns()]
#         self._build_helper_strings()
              
    def create(self, table, columns):
        """
        this method creates the table and defines the columns.  currently only
        the column name and column type are supported. (the notnull, pk, etc are not implemented)
        """
        #todo:add a more appropriate init_db() method that can utilize cursor.executescript() 
        #to run an schema.sql file
        self._table = table
        self._columns = columns
        self._build_helper_strings()
        
        with self.connection:
            logger.info('Database create()')
            self.cursor.execute('DROP TABLE IF EXISTS {}'.format(self._table))
            self.cursor.execute('CREATE TABLE {} {}'.format(self._table, self._queryStrColumnsTypes))

    def _build_helper_strings(self): 
        # 'INSERT INTO table'
        self._queryStrInsertInto = 'INSERT INTO {}'.format(self._table)
        
        #this will populate self._columns appropriately.  requires the self._table to be set ahead of time.
        self._columns = self.columns
        
        # '(col1, col2, col3, ...)'
        self._queryStrColumns = '('
        self._queryStrColumnsTypes = '('
        for column, columnType in self._columns:
            self._queryStrColumns += str(column) + ', '
            self._queryStrColumnsTypes += str(column) + ' ' + str(columnType) + ', '
        self._queryStrColumns = self._queryStrColumns[:-2] + ')'
        self._queryStrColumnsTypes = self._queryStrColumnsTypes[:-2] + ')'
        
        # '(?, ?, ?, ...)'
        self._queryStrValues = 'VALUES ('
        for _ in range(len(self._columns)):
            self._queryStrValues += '?, '
        self._queryStrValues = self._queryStrValues[:-2] + ')'
        
    def set_row_factory(self):
        self.cursor.row_factory = sqlite3.Row
        
    def unset_row_factory(self):
        self.cursor.row_factory = None

    def insert(self, table, *rows):
        """
        Insert helper method.  This method uses the initialized table and columns defined in the constructor.  Simply pass a tuple of values
        you want inserted (in proper order) and they will be inserted into the database and will be followed by a commit() to the database.
        """
        self._table = table
        self._build_helper_strings()
            
        try:
            with self.connection:
                self.cursor.execute(self._queryStrInsertInto + ' ' + self._queryStrColumns + ' ' + self._queryStrValues, *rows)
        except Exception as e:
            #todo: do something here to handle exception
            exc_tb = sys.exc_info()[2]
            logger.warning('something went wrong with insert()... {} - line {}'.format(e, exc_tb.tb_lineno))

    def retrieve(self, **kwargs):
        """
        The retrieve method returns an sqlite3 cursor handle object that can accept optional **kwargs of 'select' and 'where' that
        help define sql SELECT query.  If no **kwargs are supplied, the cursor returned is for all records in the table.
        The 'query' argument can be passed if one wants to explicitly define the query and not use the helper methods.
        """
        select = kwargs.get('select', None)
        where = kwargs.get('where', None)
        query = kwargs.get('query', None)
        
        if query is not None:
            sql = query
        elif select is None and where is None:
            sql = 'SELECT * FROM {}'.format(self._table)
        elif select is not None and where is None:
            sql = 'SELECT {} FROM {}'.format(select, self._table)
        elif select is None and where is not None:
            sql = 'SELECT * FROM {} WHERE {}'.format(self._table, where)
        elif select and where:
            sql = 'SELECT {} FROM {} WHERE {}'.format(select, self._table, where)
        
        return self.cursor.execute(sql)

    @property
    def columns(self):
        """
        consider not making this a property and keeping totally internal
        """
        try:
            row_factory = self.cursor.row_factory
            self.cursor.row_factory = None
            self.cursor.execute('PRAGMA TABLE_INFO({})'.format(self._table))
            cols = self.cursor.fetchall()
            self._columns = [(params[1], params[2]) for params in cols]
            self.cursor.row_factory = row_factory
        finally:
            return self._columns
        
    @property
    def table(self):
#         try:
#             if self._table is None:
#                 row_factory = self.cursor.row_factory
#                 self.cursor.row_factory = None
#                 self.cursor.execute('SELECT name FROM sqlite_master WHERE type="table"')
#                 self._table = self.cursor.fetchone()[0]
#                 self.cursor.row_factory = row_factory
#         finally:
        return self._table
    
    @table.setter
    def table(self, table):
        self._table = table
   
     
class __Database(object):
    """
    iteration 1 of database class - obsolete
    """
    def __init__(self, **kwargs):
        self._filename = kwargs.get('filename')
        self._db = sqlite3.connect(self._filename)
        self._db.row_factory = sqlite3.Row    
        
        logger.info('Creating Database object')
        
        if kwargs.get('table') is not None:
            self._table = kwargs.get('table')
            
        if kwargs.get('columns') is not None:
            self._columns = kwargs.get('columns')
            self._build_query_strings()
            
        if kwargs.get('create') == 'yes':
            self._create()
        
    def _create(self):     
        logger.info('Database _create()')
        sql = 'DROP TABLE IF EXISTS {}'.format(self._table)
        self._db.execute(sql)
        logger.debug(sql)
        
        sql = 'CREATE TABLE {} {}'.format(self._table, self._queryStrColumnsTypes)
        self._db.execute(sql)
        logger.debug(sql)
        
        self._db.commit()
        
    def __str__(self):
        pass
        
    def sql_do(self, sql, *args):
        """
            sqlite3.execute() method that follows up with a commit() command.  This method does not return any cursor handles.
        """
        logger.debug(sql+str(*args))
        self._db.execute(sql, *args)
        self._db.commit()
    
    def insert(self, *rows):
        """
            Insert helper method.  This method uses the initialized table and columns defined in the constructor.  Simply pass a tuple of values
            you want inserted (in proper order) and they will be inserted into the database and will be followed by a commit() to the database.
        """
        sql = self._queryStrInsertInto + ' ' + self._queryStrColumns + ' ' + self._queryStrValues
        logger.debug(sql+str(*rows))
        self._db.execute(sql, *rows)
        self._db.commit()
    
    def retrieve(self, **kwargs):
        """
        The retrieve method returns an sqlite3 cursor handle object that can accept optional **kwargs of 'select' and 'where' that
        help define sql SELECT query.  If no **kwargs are supplied, the cursor returned is for all records in the table.
        The 'query' argument can be passed if one wants to explicitly define the query and not use the helper methods.
        """
        select = kwargs.get('select', None)
        where = kwargs.get('where', None)
        query = kwargs.get('query', None)
        
        if query is not None:
            sql = query
        elif select is None and where is None:
            sql = 'SELECT * FROM {}'.format(self._table)
        elif select is not None and where is None:
            sql = 'SELECT {} FROM {}'.format(select, self._table)
        elif select is None and where is not None:
            sql = 'SELECT * FROM {} WHERE {}'.format(self._table, where)
        elif select and where:
            sql = 'SELECT {} FROM {} WHERE {}'.format(select, self._table, where)
        
        logger.debug(sql)
        cursor = self._db.execute(sql)
#         return cursor
    
    def iterate(self, **kwargs):
        """
            The iterate() method is a generator function which can be used to iterate over rows returned by the cursor.  It calls retrieve(**kwargs)
            to get a cursor handle.
        """
        cursor = self.retrieve(**kwargs)
        row = cursor.fetchone()
        while row is not None:
            yield row
            row = cursor.fetchone()
            
    def delete(self):
        pass
    
    def update(self):
        pass
    
    def __iter__(self):
        sql = 'SELECT * FROM {}'.format(self._table)
        logger.debug(sql)
        cursor = self._db.execute(sql)
        for row in cursor:
#             logger.debug(dict(row))
            yield dict(row)
            
    def _build_query_strings(self):
        # 'INSERT INTO table'
        self._queryStrInsertInto = 'INSERT INTO {}'.format(self._table)
        
        # '(col1, col2, col3, ...)'
        self._queryStrColumns = '('
        self._queryStrColumnsTypes = '('
        for column, columnType in self._columns:
            self._queryStrColumns += str(column) + ', '
            self._queryStrColumnsTypes += str(column) + ' ' + str(columnType) + ', '
        self._queryStrColumns = self._queryStrColumns[:-2] + ')'
        self._queryStrColumnsTypes = self._queryStrColumnsTypes[:-2] + ')'
        
        # '(?, ?, ?, ...)'
        self._queryStrValues = 'VALUES ('
        for _ in range(len(self._columns)):
            self._queryStrValues += '?, '
        self._queryStrValues = self._queryStrValues[:-2] + ')'

    @property
    def filename(self):
        return self._filename
    
    @property
    def table(self):
        return self._table
    
    @property
    def columns(self):
        return self._columns
    
    @property
    def count(self):
        sql = 'SELECT COUNT(*) FROM {}'.format(self._table)
        cursor = self._db.execute(sql).fetchall()
        logger.debug(sql+' - {} records'.format(cursor[0][0]))
        return cursor[0][0]
    
def main(): 
    
    db = Database('test.db')
    db.create('_table', (('col1', 'TEXT'),('col2', 'TEXT')))
    db.insert(('hello','tyson'))
    db.insert(('my name is','jobe'))
    
#     for tableInfo in db.cursor.execute('PRAGMA TABLE_INFO(_table)'):
#         print(tableInfo)
# 
#     print(db.table)
#     print(db.columns)
#     
#     for row in db.retrieve():
#         print(row)
#         
#     for row in db.retrieve(select='col1', where='col1=="hello"'):
#         print(row)
    db.cursor.row_factory = sqlite3.Row    
    for row in db.cursor.execute('SELECT col1 from _table WHERE col1=="hello"'):
        print(dict(row))

    for row in db.retrieve(where='col1=="my name is"'):
        print(dict(row))
        
    print(db.columns)
        
    print(db.table)
        
    db.connection.close()
#     conn = sqlite3.connect('test1.db', factory=DbConnect)
#     cursor = conn.cursor()
#     cursor.create('_table', (('col1', 'TEXT'),('col2', 'TEXT')) )
#     conn.commit()
#     cursor.row_factory = sqlite3.Row

#     for row in cursor.execute('select * from data_log'):
#         print(dict(row))
        
   
#     dbFile = 'test.db'
#     dbCreate = 'no' if os.path.exists(dbFile) else 'yes'
#     dbTable = 'data_log'
#     dbColumns = (('tag', 'TEXT'), ('val', 'REAL'), ('timestamp', 'REAL'))
#     db = Database(filename=dbFile, table=dbTable, columns=dbColumns, create=dbCreate)  
#     
#     db.sql_do('DELETE FROM {}'.format(db.table))
#     
#     #throw some shit into the db
#     for _ in range(30):
#         value = random.uniform(0, 30)
#         db.insert(('ozone.pressure', value, time.time()))
#         db.insert(('oxygen.flow', value, time.time()))
#     
#     #you can iterate over all rows of a database object
#     for row in db:
#         print(row)
#     print()
#     
#     #you can use the iterate helper method which yields a fetchone of the cursor (iterate calls retrieve)
#     for row in db.iterate(where='val<10'):
#         print(dict(row))
#     print()
#         
#     #same as above but different example
#     for row in db.iterate(select='{}, {}'.format(db.columns[0][0], db.columns[1][0]), where='{} < 10'.format(db.columns[1][0])):
#         print(dict(row))
#     print()
#     
#     print('record count:', db.count)
#     
#     #if you dont use the iterate helper method, you can call retrieve directly (returns the cursor)
#     c = db.retrieve(select='*', where='val > 15')
#     for row in c:
#         print(row)
#         
#     c = db.retrieve(query='pragma table_info(data_log)').fetchall()
#     print(c)
    

if __name__ == "__main__": 
    import os
    import random
    import time
    logHandler = logging.StreamHandler()
    logFormatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-5s - %(message)s')
    logger.setLevel(logging.DEBUG)
    logHandler.setFormatter(logFormatter)
    logger.addHandler(logHandler)
    main()
