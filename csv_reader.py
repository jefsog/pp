import re
import os
import csv


class CSV_reader(File_reader):
    """read csv or txt file, return a list of rows and fields"""
    def __init__(self, arg):
        super(CSV_reader, self).__init__()
        self.arg = arg
        
    # use Python CSV library to parse files
    def read(str_path_name, str_delimiter = ','):
        '''    
        '\0' is null bytes in the input file    
        '''
        
        lst_lst_field = []
        try:
            if str_delimiter == ',':
                
                with open(str_path_name, 'rb') as csvfile:
                    reader = csv.reader(csvfile)            
                    for row in reader:
                        lst_lst_field.append(row)
                
            else:
                
                with open(str_path_name, 'rb') as csvfile:
                    reader = csv.reader(csvfile,  delimiter=str_delimiter, quoting=csv.QUOTE_NONE)            
                    for row in reader:
                        #print row
                        lst_lst_field.append(row)        
        
        except csv.Error as e:
            print str_path_name + ':' + e
        return lst_lst_field



