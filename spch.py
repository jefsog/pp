import sys
import os
import re
import fl
# detect utf-8 or windows-1252

class SPCH(object):
    def __init__(self, lst_lst_field):
        path_name = os.path.abspath('spch.py')
        path = os.path.dirname(path_name)
        file_w1252 = os.path.join(path, 'w1252.txt')
        #file_utf8 = os.path.join(path, 'utf8.txt') # can not keep utf-8 and its equvalient of ISO8859 in the same file
        lst_w1252 = fl.CSV_file(file_w1252, '\t').read()
        #lst_utf8 = fl.CSV_file(file_utf8, '\t').read()
        self.dic_w1252 = self.get_dict(lst_w1252)
        self.dic_utf8 = {'\xc3\x8e':'\xce'
                        ,'\xCE\xB1':'\x61'
                        ,'\xE2\x80\x90':'\x5f'
                        ,}
        self.lst_lst_field = lst_lst_field

        




    # transform list to dictionary
    def get_dict(self, lst):
        dic = {}
        for line in lst:
            if len(line) == 2:
                dic[line[0]] = line[1]
                
        return dic

    # return the number of found pattern
    def lst_spchar(self, compiled_pattern, str_line):       
        lst_matched = re.findall(compiled_pattern, str_line)
        return lst_matched

    # browse the file row by row, field by field, looking for spchar
    # return the count of '[\x80-\xff]', utf-8, ratio and a position list of '[\x80-\xff]'
    def utf8_percent(self):
        compiled_pattern_all = re.compile('[\x80-\xff]') 
        compiled_pattern_utf8 = re.compile('[\xc2-\xf4][\x80-\xbf]{1,3}')
        
        ratio = 0
        count_all = 0
        count_utf8 = 0
        lst_spchar_position = []
        for i in range(len(self.lst_lst_field)):
            for j in range(len(self.lst_lst_field[i])):
                int_all = len(self.lst_spchar(compiled_pattern_all, self.lst_lst_field[i][j]))
                
                int_utf8 = len(self.lst_spchar(compiled_pattern_utf8, self.lst_lst_field[i][j]))
                
                count_all = count_all + int_all
                count_utf8 = count_utf8 + int_utf8
                if int_all>0:
                    tup = (i, j)
                    lst_spchar_position.append(tup)
        if count_all != 0:
            ratio = count_utf8/float(count_all)
        return ( ratio , count_utf8, count_all, lst_spchar_position)
    
    def shm_detector(self):
        str_encoding_type = 'ASCII'
        error_info = None
        lst_spchar_position = self.utf8_percent()[3]
        if len(lst_spchar_position) > 0:
            for position in lst_spchar_position:
                try:
                    self.lst_lst_field[position[0]][position[1]].decode('utf-8')
                    str_encoding_type = 'utf-8'
                except UnicodeDecodeError as e:
                    error_info = e
                    str_encoding_type = 'windows-1252'

        return (str_encoding_type, error_info)

    def change_encoding(self, lst_spchar_position, compiled_pattern, dic_encoding):
        lst_replace_result = []
        
        for position in lst_spchar_position:
            
            lst_matched = self.lst_spchar(compiled_pattern, self.lst_lst_field[position[0]][position[1]])
            #print self.lst_lst_field[position[0]][position[1]]
            
            if lst_matched: # not an empty list
                for hex_value in lst_matched:
                    # get the segment including the sp char
                    str_field = self.lst_lst_field[position[0]][position[1]]
                    int_position = str_field.find(hex_value)
                    field_length = len(str_field)
                    start_position = int_position-50
                    if start_position < 0:
                        start_position = 0
                    ending_position = int_position+50
                    if ending_position > field_length:
                        ending_position = field_length
                    str_segment = str_field[start_position:ending_position]
                    
                    


                    # if hex_value in dic_encoding:
                    #     self.lst_lst_field[position[0]][position[1]] = self.lst_lst_field[position[0]][position[1]].replace(hex_value, dic_encoding[hex_value])
                    #     tup_result = (position[0], position[1], hex_value, dic_encoding[hex_value], str_segment)
                    # else:
                                               
                    #     tup_result = (position[0], position[1], hex_value, None, str_segment)
                    
                    # to be safe, the above character replacing code was commented out
                    # if using the character replacing code again, comment the following line out
                    self.lst_lst_field[position[0]][position[1]] = self.lst_lst_field[position[0]][position[1]].replace(hex_value, str([hex_value])) # make hex value unchanged in oracle
                    #self.lst_lst_field[position[0]][position[1]] = self.lst_lst_field[position[0]][position[1]].decode('utf-8')
                    tup_result = (position[0], position[1], hex_value, None, str_segment)


                    lst_replace_result.append(tup_result)
        return lst_replace_result                  
    
    

    def replace_spch(self):
        lst_replace_result = []
        tup_detect_result = self.utf8_percent()
        compiled_pattern_w1252 = re.compile('[\x80-\x9f]') 
        compiled_pattern_utf8 = re.compile('[\xc2-\xf4][\x80-\xbf]{1,3}')
      
        if tup_detect_result[2] > 0:
            if tup_detect_result[0] > 0.25:
                # utf-8
                lst_replace_result = self.change_encoding(tup_detect_result[3], compiled_pattern_utf8, self.dic_utf8)
            else:
                # w1252
                lst_replace_result = self.change_encoding(tup_detect_result[3], compiled_pattern_w1252, self.dic_w1252) 
        return (tup_detect_result[0:3], lst_replace_result)

if __name__ == '__main__':
    
    #lst_line = read_file(sys.argv[1]) # accept a path pointing to a text file
    lst_line = fl.CSV_file(r'C:\Users\jesong\csv\HMIR2014MAR16_part.txt').read()
    #for line in lst_line:
        #print lst_line
    ins_sp = SPCH(lst_line)
    print ins_sp.utf8_percent()
    #print result


    
        