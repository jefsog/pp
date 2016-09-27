import sys
import re

# detect utf-8 or windows-1252


# looking for special character in a file, so starting by read file
def read_file(file_name):
    file = open(file_name, 'rU')
    lst_line = []
    for line in file:
        lst_line.append(line)
    file.close()
    return lst_line

# return the number of found pattern
def count_spchar(compiled_pattern, str_line):       
    lst_matched = re.findall(compiled_pattern, str_line)
    return len(lst_matched)

#'[\xc0-\xdf][\x80-\xbf]{1,3}' is the pattern of utf-8
def utf8_possibility(lst_line):
    compiled_pattern_all = re.compile('[\x80-\xff]') 
    compiled_pattern_utf8 = re.compile('[\xc0-\xdf][\x80-\xbf]{1,3}')
    count_all = 0
    count_utf8 = 0

    for line in lst_line:
        count_all = count_all + count_spchar(compiled_pattern_all, line)
        count_utf8 = count_utf8 + count_spchar(compiled_pattern_utf8, line)
    return count_utf8/float(count_all)


# return list of tuple(line#, column#, special char)
def locate_spchar(lst_line):
    lst_spchar = []
    for i in range(len(lst_line)):
        line = lst_line[i]
        for j in range(len(line)):
            char = line[j]
                    
            if is_spchar(char):
                tup = (i,j,char)
                
                lst_spchar.append(tup)
            
    return lst_spchar



if __name__ == '__main__':
    
    lst_line = read_file(sys.argv[1]) # accept a path pointing to a text file

    print utf8_possibility(lst_line)
    
        