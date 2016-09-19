import mysql.connector
from mysql.connector import errorcode
import mysql_con

TABLES = {}
TABLES['jeff'] = (
    "CREATE TABLE marina ("
    "  jeff varchar(4000) "
    ") ENGINE=InnoDB")




cnx = mysql_con.get_connection()
'''
cnx = mysql.connector.connect(user='root', password='',
                              host='127.0.0.1',
                              database='eris')
'''
cursor = cnx.cursor()

for name, ddl in TABLES.iteritems():
    try:
        print("Creating table {}: ".format(name))
        cursor.execute(ddl)
        #cursor.execute('commit')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cursor.close()
cnx.close()