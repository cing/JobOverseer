"""
This is a simple script to select queue_data from sqllite
and generate a usage report for a given timeline

Just kidding, right now it just dumps a database.
"""

import sqlite3
filename = "queue_data.db"
conn = sqlite3.connect(filename)
c = conn.cursor()
c.execute('select * from jobs')
for row in c:
    print row

c.close()
