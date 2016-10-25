import re
import codecs
import os
import csv
import datetime
#from openpyxl import Workbook
#from openpyxl import load_workbook

#return a list of absolute path name in a directory
def read_directory(str_absolute_path):
    lst_file_name = os.listdir(str_absolute_path)
    lst_absolute_path_name = []
    for name in lst_file_name:

        if name[0:8] != 'load_log':
            lst_absolute_path_name.append(os.path.join(str_absolute_path, name)) 
    return lst_absolute_path_name

def write_log(file_name, str_to_write):
    #file_name = absolute_path + '\load_log_' + datetime.datetime.now().strftime("%m%d_%H_%M_%S") + '.txt'
    
    file = open(file_name, 'w')
    file.write(str_to_write)
    file.close()

def write_log_utf8(file_name, str_to_write):
    file = codecs.open(file_name, encoding='utf-8', mode = 'wb')
    ustr = str_to_write.decode('utf-8')
    file.write(ustr)
    file.close()


class File(object):
    """read or write csv/txt file"""
    def __init__(self, file_name):
        self.file_name = file_name
        
    def copy_list(self, lst_list):
        lst_cloan = []
        for item in lst_list:
            lst_cloan.append(item)
        return lst_cloan

    def clean_field(self, str_field):
        if str_field[0] == ',':
            str_field = str_field[1:]
        if len(str_field) > 1 and str_field[0] == '"' and str_field[-1] == '"':
            str_field = str_field[1:-1]
        str_field = str_field.replace('""', '"')
        return str_field

    #read a txt file, return a list of lines in the file
    def read_file(self): 
        file_in = open(self.file_name, 'rU')
        lst_line = []
        for line in file_in:
            lst_line.append(line)
        file_in.close()
        return lst_line

    #return a list of max length of each column in the parsed file
    def get_fields_max_length(self, lst_lst_file, int_starting_position): 
        lst_field_max_length = []
        for j in range(int_starting_position, len(lst_lst_file)): #skip the file head, start from the second line
            line = lst_lst_file[j]
            i = 0
            for field in line:
                if len(lst_field_max_length) <= i:
                    lst_field_max_length.insert(i, -1) #fill the empty list with -1 in order to compare 
                if len(field) > lst_field_max_length[i]:
                    lst_field_max_length[i] = len(field)
                i = i+1
        
        return lst_field_max_length

    


    def get_data_type(self, lst_field_length):
        lst_data_type = []
        for num in lst_field_length:
            if num<=4000:
                if num == 0:  #zero length varchar is not allowed
                    num = 1 
                lst_data_type.append('varchar('+str(num)+')')
            else:
                lst_data_type.append('clob')
                
        return lst_data_type






    # if the column is delimited by space, replace the space with underscore
    def add_underscore(self, lst_string):    
        lst_underscore_added = []
        for content in lst_string:
            content = content.strip()
            content = re.sub('[^A-z0-9_]', '_', content)
            
            while content.find('__') > -1:
                content = content.replace('__', '_')    
        
            if len(content) > 30:
                content = content[0:30]
            lst_underscore_added.append(content) 
        return lst_underscore_added

    # retrieve file name, prefix it with a string, trunk its length to no more than 30
    def get_table_name(self, str_absolute_file_name, str_directory, str_prefix):
        int_dot_position = str_absolute_file_name.find('.')
        
        if  len(str_prefix) > 0:
            table_name = str_prefix + '_' + str_absolute_file_name[len(str_directory)+1:int_dot_position]
        else:
            table_name = str_absolute_file_name[len(str_directory)+1:int_dot_position]
        
        table_name = re.sub('[^A-z0-9_]', '_', table_name)
        while table_name.find('__') > -1:
            table_name = table_name.replace('__', '_')    
        
        if len(table_name) > 30:
            table_name = table_name[0:30]
        return table_name


class CSV_file(File):
    """read csv or txt file, return a list of rows and fields"""
    def __init__(self, file_name, str_delimiter = ','):
        super(CSV_file, self).__init__(file_name)
        self.str_delimiter = str_delimiter
        
    
    # use Python CSV library to parse files
    def read(self):
        '''    
        '\0' is null bytes in the input file    
        '''
        csv.field_size_limit(500*1024*1024)
        lst_lst_field = []
        try:
            if self.str_delimiter == ',':
                
                with open(self.file_name, 'rb') as csvfile:
                    reader = csv.reader(csvfile)            
                    for row in reader:
                        lst_lst_field.append(row)
                
            else:
                
                with open(self.file_name, 'rb') as csvfile:
                    reader = csv.reader(csvfile,  delimiter=self.str_delimiter, quoting=csv.QUOTE_NONE)            
                    for row in reader:
                        #print row
                        lst_lst_field.append(row)        
        
        except csv.Error as e:
            raise e
        return lst_lst_field

# file size over 50MB
# feed no more than 200k records for one batch
class CSV_list(File):
    
    def __init__(self, line_list, str_delimiter = ','):        
        self.str_delimiter = str_delimiter
        self.line_list = line_list
    
    def read(self):
        '''    
        '\0' is null bytes in the input file    
        '''
        lst_lst_field = []
        try:
            reader = None
            if self.str_delimiter == ',':
                
                reader = csv.reader(self.line_list)            
                
                
            else:
                
                
                reader = csv.reader(self.line_list,  delimiter=self.str_delimiter, quoting=csv.QUOTE_NONE)            
            for row in reader:
                
                lst_lst_field.append(row)        
        
        except csv.Error as e:
            raise e
        return lst_lst_field



# class Excel_file(File):
#     def parse_excel():
#         wb2 = load_workbook(filename = 'C:\Users\jesong\pp\\file_to_load\\ExcelExport.xlsx', read_only = True, data_only = True)
#         name = wb2.get_sheet_names()[0]

#         ws = wb2.active
#         lst_lst_parsed_file = []
#         for row in ws.rows:
#             lst_field = []
#             for cell in row:
#                 lst_field.append(str(cell.value))
#             #print lst_field
#             lst_lst_parsed_file.append(lst_field)   

#         return lst_lst_parsed_file

if __name__ == '__main__':
    
    file = File('try')
    lst = ['[_\\xef\\xbb\\xbf_]']
    print file.add_underscore(lst)

    #f = Big_CSV(r'Z:\Eris\Documentation\ERISDirect\TestSearches_EDR\OSHA\2016_Sep23\OSHA_Violations\osha_violation.csv')
    # file_name = r'Z:\Eris\Documentation\ERISDirect\TestSearches_EDR\OSHA\2016_Sep23\OSHA_Violations\osha_violation.csv'
    # f = open(file_name, 'rb')
    # lst_line = []
    # count = 0
    # for line in f:
    #     if count < 200000:
    #         lst_line.append(line)
    #         if count%10000 == 0: print count
    #         count += 1
    #     else:
    #         break
    # f.close()
    # reader = csv.reader(lst_line)
    # parsed_csv = []  
    # for row in reader:
    #     parsed_csv.append(row)

    
    # for i in range(len(parsed_csv)):
    #     if i%10000 ==0:
    #         print i
    #         print parsed_csv[i]

    # print 'over'




