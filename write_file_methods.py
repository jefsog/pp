import sys
import datetime


def write_log(absolute_path, str_to_write):
    file_name = absolute_path + '\load_log_' + datetime.datetime.now().strftime("%m%d_%H_%M_%S") + '.txt'
    
    file = open(file_name, 'w')
    file.write(str_to_write)
    file.close()



if __name__ == '__main__':
	write_log(sys.argv[1], sys.argv[2])