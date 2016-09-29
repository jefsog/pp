import sys
import time
import fl
import db
import spch



def normal_load(ins_db, str_absolute_path, file_name, str_prefix, str_delimiter):
    spch_treated_result = []
    str_log = ''
    print file_name + ': loading started'
    start_time = time.time()
    str_log = str_log + 'file name: ' + file_name + '\n'
    try:        
        #use csv module parse file
        ins_fl = fl.CSV_file(file_name, str_delimiter)
        lst_lst_parsed_file = ins_fl.read()

        #special character treatment
        ins_spch = spch.SPCH(lst_lst_parsed_file)
        spch_treated_result = ins_spch.replace_spch()
        lst_lst_parsed_file = ins_spch.lst_lst_field

        #prepare creating table
        lst_column_max_length = ins_fl.get_fields_max_length(lst_lst_parsed_file) 
        lst_field_type = ins_fl.get_data_type(lst_column_max_length)
        lst_column_name = ins_fl.add_underscore(lst_lst_parsed_file[0])        
        table_name = ins_fl.get_table_name(file_name, str_absolute_path, str_prefix)
        str_log = str_log + 'table name: ' + table_name + '\n'
        
        # drop table, then create table, and insert rows
        ins_db.drop_table(table_name)
        ins_db.create_table(table_name, lst_column_name, lst_field_type)
        int_rows_inserted = ins_db.insert_rows(table_name, lst_lst_parsed_file, 1)
        
        str_log = str_log + 'rows inserted: ' + str(int_rows_inserted) + '\n'

    except Exception as e:
        print file_name + ': loading fails!'
        print e
        str_log = str_log + repr(e) + '\n'
        
    
    end_time = time.time()        
    str_seconds =  'Seconds taken: ' + str(end_time - start_time)
    print str_seconds
    str_log = str_log + str_seconds + '\n'
    for tup in spch_treated_result:
        str_log = str_log + str(tup) + '\n'
    return str_log

def main(str_absolute_path, str_prefix, str_delimiter = ','):
    # read file names in directory
    lst_file = fl.read_directory(str_absolute_path)
    str_log = ''
    
    # prepare database connection
    #ins_db = db.US_database()
    ins_db = db.Test_database()
    
    for file_name in lst_file:
        str_log = normal_load(ins_db, str_absolute_path, file_name, str_prefix, str_delimiter)        
    
    ins_db.close()
    fl.write_log(str_absolute_path, str_log)

if __name__ == '__main__':
    
    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2], str_delimiter = ',')
    elif len(sys.argv) == 4:        
        str_delimiter = sys.argv[3].decode('string_escape')  # make '\' working as an escape character
        main(sys.argv[1], sys.argv[2], str_delimiter)
        
    else:
        print 'Correct format:'
        print 'Python ' + sys.argv[0] + ' absolute_path' + ' prefix ' + '[' +'delimiter' + ']'







    
    # str_log = ''
    # ins_db = db.US_database()
    
    # file_name = r'Z:\Eris\Documentation\ERISDirect\TestSearches_EDR\OSHA\2016_Sep23\OSHA_Violations\osha_violation.csv'
    # start_time = time.time()
    # str_log = str_log + 'file name: ' + file_name + '\n'
    # try:        
    #     ins_fl = fl.CSV_file(file_name)
    #     print 'start reading'
    #     lst_lst_parsed_file = ins_fl.read()
    
    #     lst_column_max_length = ins_fl.get_fields_max_length(lst_lst_parsed_file) 
    #     lst_field_type = ins_fl.get_data_type(lst_column_max_length)
    #     lst_column_name = ins_fl.add_underscore(lst_lst_parsed_file[0])        
    #     table_name = ins_fl.get_table_name(file_name, str_absolute_path, str_prefix)
    #     str_log = str_log + 'table name: ' + table_name + '\n'
    #     ins_db.drop_table(table_name)
    #     ins_db.create_table(table_name, lst_column_name, lst_field_type)
    #     int_rows_inserted = ins_db.insert_rows(table_name, lst_lst_parsed_file, 1)
    #     str_log = str_log + 'rows inserted: ' + str(int_rows_inserted) + '\n'
    # except Exception as e:
    #     print file_name + ': loading fails!'
    #     print e
    #     str_log = str_log + repr(e) + '\n'
            
        
    # end_time = time.time()        
    # str_seconds =  'Seconds taken: ' + str(end_time - start_time)
    # print str_seconds
    # str_log = str_log + str_seconds + '\n'
    # fl.write_log('Z:\Eris\Documentation\ERISDirect\TestSearches_EDR\OSHA\2016_Sep23\OSHA_Violations', str_log)        


        