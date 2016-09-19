import sys
import time
import read_file_methods
import database_methods
import write_file_methods

def main(str_absolute_path, str_prefix, str_delimiter = ','):
    lst_file = read_file_methods.read_directory(str_absolute_path)
    str_log = ''
    for file_name in lst_file:
        print file_name + ': loading started'
        start_time = time.time()
        str_log = str_log + 'file name: ' + file_name + '\n'
        try:        
            lst_lst_parsed_file = read_file_methods.csv_reader(file_name, str_delimiter)            
            lst_column_max_length = read_file_methods.get_fields_max_length(lst_lst_parsed_file) 
            lst_field_type = read_file_methods.get_data_type(lst_column_max_length)
            lst_column_name = read_file_methods.add_underscore(lst_lst_parsed_file[0])        
            table_name = read_file_methods.get_table_name(file_name, str_absolute_path, str_prefix)
            str_log = str_log + 'table name: ' + table_name + '\n'
            database_methods.drop_table(table_name)
            str_error = database_methods.create_table(table_name, lst_column_name, lst_field_type)
            int_rows_inserted = database_methods.insert_rows(table_name, lst_lst_parsed_file, 1)
            str_log = str_log + 'rows inserted: ' + str(int_rows_inserted) + '\n'
        except Exception as e:
            print file_name + ': loading fails!'
            print e
            str_log = str_log + repr(e) + '\n'
            
        
        end_time = time.time()        
        str_seconds =  'Seconds taken: ' + str(end_time - start_time)
        print str_seconds
        str_log = str_log + str_seconds + '\n'
    write_file_methods.write_log(str_absolute_path, str_log)

if __name__ == '__main__':
    '''
    directory = 'C:\Users\jesong\pp\csv'
    main(directory, 'jeff')
    '''
    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2], str_delimiter = ',')
    elif len(sys.argv) == 4:        
        str_delimiter = sys.argv[3].decode('string_escape')  # make '\' working as an escape character
        main(sys.argv[1], sys.argv[2], str_delimiter)
        
    else:
        print 'Correct format:'
        print 'Python ' + sys.argv[0] + ' absolute_path' + ' prefix ' + '[' +'delimiter' + ']'


        
    '''
        #print lst_lst_parsed_file
        print len(lst_column_name)
        print len(lst_field_type)
        print len(lst_column_max_length)
        
        
        for i in range(len(lst_lst_parsed_file)):
            if len(lst_lst_parsed_file[i]) == 13:
                print i
                print lst_lst_parsed_file[i]

    '''
        
        #print 'inserted'
        