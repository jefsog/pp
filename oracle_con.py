import cx_Oracle
#con = cx_Oracle.connect('pythonhol/welcome@127.0.0.1/orcl')
#con = cx_Oracle.connect('eris/eris@cabcvan1ora001.glaciermedia.inc/GMPROD')

#con = cx_Oracle.connect('eris/eris@gmtest')
#con = cx_Oracle.connect('eris_us_load/eris@GMPROD')
#print con.version
#con.close()



def get_connection():

#    cnx = cx_Oracle.connect('eris/eris@gmtest')
    cnx = cx_Oracle.connect('eris_us_load/eris@GMPROD')
    return cnx