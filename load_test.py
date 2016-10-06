import sys
import os
import time
import fl
import db
import spch



def normal_load(ins_db, str_absolute_path, file_name, str_prefix, str_delimiter, int_record_starting):
    spch_treated_result = ()
    str_log = ''
    lst_column_name = []
    log_name = file_name[:file_name.find('.')] + '_failed.txt'

    print file_name + ': loading started'
    start_time = time.time()
    str_log = str_log + 'file name: ' + file_name + '\n'
    try:        
        #use csv module parse file
        ins_fl = fl.CSV_file(file_name, str_delimiter)
        lst_lst_parsed_file = ins_fl.read()

        #special character treatment
        ins_spch = spch.SPCH(lst_lst_parsed_file)
        #sherman's idea of detecting encoding
        tup_shm_detector = ins_spch.shm_detector()

        spch_treated_result = ins_spch.replace_spch()
        lst_lst_parsed_file = ins_spch.lst_lst_field

        #prepare creating table
        
        lst_column_max_length = ins_fl.get_fields_max_length(lst_lst_parsed_file, int_record_starting) 
        lst_field_type = ins_fl.get_data_type(lst_column_max_length)
        
        
        
        if int_record_starting-1 < 0:
            
            for i in range(len(lst_column_max_length)):
                
                lst_column_name.append("Field"+str(i))
        else:    
            lst_column_name = ins_fl.add_underscore(lst_lst_parsed_file[int_record_starting-1])        
        

        table_name = ins_fl.get_table_name(file_name, str_absolute_path, str_prefix)
        str_log = str_log + 'table name: ' + table_name + '\n'
        
        # drop table, then create table, and insert rows
        ins_db.drop_table(table_name)
        ins_db.create_table(table_name, lst_column_name, lst_field_type)
        int_rows_inserted = ins_db.insert_rows(table_name, lst_lst_parsed_file, int_record_starting)
        
        str_log = str_log + 'rows inserted: ' + str(int_rows_inserted) + '\n'
        log_name = file_name[:file_name.find('.')] + '_loaded.txt'
    except Exception as e:
        
        str_log = str_log + repr(e) + '\n'
        
    
    end_time = time.time()        
    str_seconds =  'Seconds taken: ' + str(end_time - start_time)
    print str_seconds
    str_log = str_log + str_seconds + '\n'
    str_log = str_log + str(spch_treated_result[0]) + '\n'
    str_log += 'Sherman detector: ' + str(tup_shm_detector) + "\n"
    for tup in spch_treated_result[1]:
        str_log = str_log + lst_column_name[tup[1]] + '\t' + str(tup) + '\n'
    fl.write_log(log_name, str_log)


def giant_load(ins_db, str_absolute_path, file_name, str_prefix, str_delimiter, int_record_starting, batch_size):
    spch_treated_result = ()
    str_sp_treatment = ''
    str_log = ''
    lst_column_name = []
    log_name = file_name[:file_name.find('.')] + '_failed.txt'

    print file_name + ': loading started'
    start_time = time.time()
    str_log = str_log + 'file name: ' + file_name + '\n'

    lst_line_for_parsing_column_name = []
    lst_column_name = []
    lst_line = []
    lst_fields_length = []
    lst_lst_field = []
    ins_csv_list = None
    
    

    try:
    # determine the length of fields

        count = 0
        f = open(file_name, 'rb')
        for line in f:
            # must have at least two lines for csv to parse
            if int_record_starting > 0 and count >= int_record_starting-1 and count < int_record_starting+1:
                lst_line_for_parsing_column_name.append(line)
                
            
            if count >= int_record_starting:
                lst_line.append(line)
            
            if count > 0 and count%batch_size == 0:

                ins_csv_list = fl.CSV_list(lst_line, str_delimiter)
                lst_lst_field = ins_csv_list.read()                            
                lst_line = []
                
                # Special treatment could use two characters to replace one
                # The special character treatment was put here 
                # is to get the length of field after treament
                ins_spch = spch.SPCH(lst_lst_field)
                ins_spch.replace_spch()
                lst_lst_field = ins_spch.lst_lst_field

                lst_fields_length.append(ins_csv_list.get_fields_max_length(lst_lst_field, 0))
            count += 1
        if len(lst_line) > 0: # the last batch could be empty
            ins_csv_list = fl.CSV_list(lst_line, str_delimiter)
            lst_lst_field = ins_csv_list.read()

            #special character treatment
            ins_spch = spch.SPCH(lst_lst_field)
            ins_spch.replace_spch()
            lst_lst_field = ins_spch.lst_lst_field

            lst_fields_length.append(ins_csv_list.get_fields_max_length(lst_lst_field, 0))

        lst_column_max_length = get_max_list(lst_fields_length)
        

        # read column name
        if int_record_starting-1 < 0:
            
            for i in range(len(lst_column_max_length)):
                
                lst_column_name.append("Field"+str(i))
        else:    
            ins_csv_list = fl.CSV_list(lst_line_for_parsing_column_name, str_delimiter)
            lst_containing_column_name = ins_csv_list.read()
            lst_column_name = lst_containing_column_name[0]

        
        
        # create table
        table_name = ins_csv_list.get_table_name(file_name, str_absolute_path, str_prefix)
        
        if len(lst_column_name) == len(lst_column_max_length):
            lst_field_type = ins_csv_list.get_data_type(lst_column_max_length)
            lst_column_name = ins_csv_list.add_underscore(lst_column_name)
            
            str_log = str_log + 'table name: ' + table_name + '\n'

            # drop table, then create table, and insert rows
            ins_db.drop_table(table_name)
            ins_db.create_table(table_name, lst_column_name, lst_field_type)
        else:
            raise Exception('column number does not match field number!')
        
        # insert rows
        count = 0
        int_rows_inserted = 0
        lst_line = []
        lst_lst_field = []
        f = open(file_name, 'rb')                
        for line in f:
            if count >= int_record_starting:
                lst_line.append(line)
                
            if count != 0 and count%batch_size == 0:
                ins_csv_list = fl.CSV_list(lst_line, str_delimiter)
                lst_lst_field = ins_csv_list.read()
                lst_line = []

                #special character treatment
                ins_spch = spch.SPCH(lst_lst_field)
                #sherman's idea of detecting encoding
                tup_shm_detector = ins_spch.shm_detector()
                spch_treated_result = ins_spch.replace_spch()
                lst_lst_field = ins_spch.lst_lst_field

                str_sp_treatment += str(spch_treated_result[0]) + '\n'
                str_sp_treatment += 'Sherman detector: ' + str(tup_shm_detector) + "\n"
                for tup in spch_treated_result[1]:
                    
                    str_sp_treatment += lst_column_name[tup[1]] + '\t' + str(tup) + '\n'
                
                str_sp_treatment += str(count) + '\n'

                int_rows_inserted += ins_db.insert_rows(table_name, lst_lst_field, 0)
                
            count += 1
        f.close()
        if len(lst_line) > 0:
            ins_csv_list = fl.CSV_list(lst_line, str_delimiter)
            lst_lst_field = ins_csv_list.read()
            lst_line = []
            
            #special character treatment
            ins_spch = spch.SPCH(lst_lst_field)
            #sherman's idea of detecting encoding
            tup_shm_detector = ins_spch.shm_detector()
            spch_treated_result = ins_spch.replace_spch()
            lst_lst_field = ins_spch.lst_lst_field
            
            str_sp_treatment += str(spch_treated_result[0]) + '\n'
            str_sp_treatment += 'Sherman detector: ' + str(tup_shm_detector) + "\n"
            for tup in spch_treated_result[1]:
                str_sp_treatment += lst_column_name[tup[1]] + '\t' + str(tup) + '\n'

            int_rows_inserted += ins_db.insert_rows(table_name, lst_lst_field, 0)
        

        str_log = str_log + 'rows inserted: ' + str(int_rows_inserted) + '\n'
        log_name = file_name[:file_name.find('.')] + '_loaded.txt'                  
    except Exception as e:
        
        str_log = str_log + repr(e) + '\n'


    end_time = time.time()        
    str_seconds =  'Seconds taken: ' + str(end_time - start_time)
    print str_seconds
    str_log = str_log + str_seconds + '\n'
    str_log += str_sp_treatment 
    fl.write_log(log_name, str_log)

def is_toload(file_name, lst_file):
    bln = True

    if file_name[file_name.find('.')+1:] not in ('txt','csv'):
        bln = False
    elif file_name[file_name.find('.')-6:file_name.find('.')] in ('loaded', 'failed'):
        bln = False
    else:
        for f in lst_file:
            if f == file_name[:file_name.find('.')] + '_loaded.txt' :
                bln = False
    return bln
# retrieve max item value from several list
# return a list of max value
def get_max_list(lst_lst_int):
    length = None
    lst_max_value = [] 
    for lst_int in lst_lst_int:
        
        if not length:
            
            length = len(lst_int)
            lst_max_value = lst_int
        elif len(lst_int) != length:
            
            raise Exception("different rows contain different number of fields")
        else:
            for i in range(length):
                if lst_max_value[i] < lst_int[i]:
                    lst_max_value[i] = lst_int[i]

    return lst_max_value


def main(str_absolute_path, str_prefix = '', str_delimiter = ',', int_record_starting = 1):
    # read file names in directory
    lst_file = fl.read_directory(str_absolute_path)
    
    
    # prepare database connection
    #ins_db = db.US_database()
    ins_db = db.Test_database()
    #int_record_starting = 1 # data, not the header, starting from the second line
    int_record_starting = int(int_record_starting)
    for file_name in lst_file:
        if is_toload(file_name, lst_file):
            try:
                file_info = os.stat(file_name)
                file_size = file_info.st_size
                # 100*1024*1024  100MB
                size_limit = 100*1024
                int_round_batch = 10
                if file_size < size_limit:
                    # size < 100 MB
                    normal_load(ins_db, str_absolute_path, file_name, str_prefix, str_delimiter, int_record_starting) 
                else:

                    # size >= 100 MB
                    total_line = 0
                    file = open(file_name, 'rb')
                    for line in file:
                        total_line += 1
                    file.close()
                    
                    loading_times = file_size/size_limit + 1
                    
                    batch_size = total_line/loading_times
                    batch_size = batch_size/int_round_batch*int_round_batch
                    
                    giant_load(ins_db, str_absolute_path, file_name, str_prefix, str_delimiter, int_record_starting, batch_size)
                   
                    

                    

            except Exception as e:
                print file_name + ': loading fails!'
                print e
                 
        
    ins_db.close()
    

if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
    elif len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])

    elif len(sys.argv) == 4:        
        str_delimiter = sys.argv[3].decode('string_escape')  # make '\' working as an escape character
        main(sys.argv[1], sys.argv[2], str_delimiter)
    elif len(sys.argv) == 5:  
        str_delimiter = sys.argv[3].decode('string_escape')  # make '\' working as an escape character
        main(sys.argv[1], sys.argv[2], str_delimiter, sys.argv[4])          
    else:
        print 'Correct format:'
        print 'Python ' + sys.argv[0] + ' absolute_path' + ' prefix ' + '[' +'delimiter' + 'records starting row number' +']'







    
   
        