import sys
import re

# locate special characters position


# looking for special character in a file, so starting by read file
def read_file(file_name):
	file = open(file_name, 'rU')
	lst_line = []
	for line in file:
		lst_line.append(line)
	file.close()
	return lst_line

# [\x80-\x9f] is the conflicting zone between Windows-1252 and ISO-8859-1
def is_spchar(str_char):	
	compiled_pattern = re.compile('[\x80-\x9f]')	
	is_matched = compiled_pattern.match(str_char)
	return is_matched

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

	for tup in locate_spchar(lst_line):
		print tup
	
		