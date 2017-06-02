#!/usr/bin/env python
from database import Database

db = Database('logs.db')
with open('create_tables.sql', 'r') as sql:
    with db.connection:
        for line in sql.readlines():
            db.cursor.execute(line)
db.connection.close()