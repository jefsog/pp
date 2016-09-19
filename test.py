import csv
import sys

def main(str_path_name, str_delimiter = ','):
    lst_lst_field = []
    with open(str_path_name, 'rb') as csvfile:
        reader = csv.reader(csvfile,  delimiter=str_selimiter, quoting=csv.QUOTE_NONE)
        
        for row in reader:
            lst_lst_field.append(row)
            

    return lst_lst_field

if __name__ == '__main__':

	print sys.argv[1]
	'''
	for row in  main('C:\Users\jesong\pp\csv\TankList.txt', '\t'):
		print len(row)
	'''