import mysql.connector

def get_connection():
    cnx = mysql.connector.connect(user='root', password='',
                              host='127.0.0.1',
                              database='eris')
    return cnx

