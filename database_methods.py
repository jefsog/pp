import cx_Oracle

import oracle_con
import load_error

def drop_table(table_name):
    query = 'drop table ' + table_name
    try: 
        cnx = oracle_con.get_connection()
        cursor = cnx.cursor()
        cursor.execute(query)
        cursor.close()
        cnx.close()
    except cx_Oracle.DatabaseError as e:        
        print query
        print e
        

def create_table(table_name, lst_column, lst_data_type):     
    
    query1 = 'create table ' + table_name + ' ('
    #print table_name
    query2 = ''
    count = -1

    if len(lst_column) == len(lst_data_type) :
        count = len(lst_column)
    else:
        print table_name + ' has ' + str(len(lst_column)) + 'columns'
        print ', but some records have ' + str(len(lst_column_length)) + ' fields'
        print ', delimiting was not sucessful.'
    for i in range(0, count):
        if len(lst_column[i]) == 0:
            lst_column[i] = 'field' + str(i)
        query2 = query2 + lst_column[i] + ' ' + lst_data_type[i] 
        if i < count-1:
            query2 = query2 + ','
    query3 = ')'    
    query = query1+query2+query3
    
    
    try:
        cnx = oracle_con.get_connection()
        cursor = cnx.cursor()
        cursor.execute(query)
        cursor.close()
        cnx.close()
    except cx_Oracle.DatabaseError as e:        
        print table_name
        print e
        print query
        return query

def insert_one_row(table_name, lst_field):
    query1 = 'insert into ' + table_name + ' values ('
    query2 = ''
    lst_length = len(lst_field)
    for i in range(0, lst_length):
        query2 = query2 + "'" + lst_field[i] + "'"
        if i < lst_length-1:
            query2 = query2 + ','
    query3 = ')'
    query = query1 + query2 + query3
    #print query
    cnx = oracle_con.get_connection()
    cursor = cnx.cursor()
    cursor.execute(query)
    cursor.execute('commit')
    cursor.close()
    cnx.close()


def insert_rows(table_name, lst_lst_field, int_start_row):
    query1 = 'insert into ' + table_name + ' values ('
    
    query3 = ')'
    try:
        cnx = oracle_con.get_connection()
        cursor = cnx.cursor()
        count = 0
        for i in range(int_start_row, len(lst_lst_field)):
            row = lst_lst_field[i]
            query2 = ''
            for j in range(len(row)):
                query2 = query2 + prepare_field(row[j]) 
                if j < len(row)-1:
                    query2 = query2 + ','
            query = query1 + query2 + query3                
            cursor.execute(query)
            count = count + 1
        cursor.execute('commit')
        cursor.close()
        cnx.close()
        print 'row_inserted: ' + str(count)
        return count
    except cx_Oracle.DatabaseError as e:
        raise cx_Oracle.DatabaseError(table_name+"|"+query)
        #print table_name
        # print e
        # print query

def prepare_field(str_string):
    string_returned = ''
    if len(str_string)<=4000:
        if str_string.find("'") == -1:
            string_returned = "'" + str_string + "'"
        else:
            string_returned = "'" + escape_single_quotation(str_string) + "'"
    else:
        if str_string.find("'") == -1:
            string_returned = prepare_clob_field(str_string)
        else:
            string_returned = prepare_clob_field(escape_single_quotation(str_string))
    return string_returned

# if a field contains a single quotation mark, escapte it with another single quotation mark
def escape_single_quotation(str_string):
    str_string = str_string.replace("'", "''") 
    return str_string

#string with length over 4000 can not be transmitted into Oracle 
#so it is sliced first, then wrap with to_clob, finally concatnated 
def prepare_clob_field(str_field):
    int_step = 4000
    i = 0
    str_code = ''
    while i < len(str_field):
        str_code = str_code  + "to_clob('"+str_field[i:(i+int_step)]+"')"
        i = i + int_step
        if i<len(str_field):
            str_code = str_code + '||'
    return str_code



        
