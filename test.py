import csv
import sys
import re
import os
import codecs
def main(str_path_name, str_delimiter = ','):
    lst_lst_field = []
    with open(str_path_name, 'rb') as csvfile:
        reader = csv.reader(csvfile,  delimiter=str_selimiter, quoting=csv.QUOTE_NONE)
        
        for row in reader:
            lst_lst_field.append(row)
            

    return lst_lst_field


def outer(insider):
    insider
    print 'out'

def insider():
    print 'in'

if __name__ == '__main__':

    outer(insider())



    #print sys.argv[1]
    '''
    for row in  main('C:\Users\jesong\pp\csv\TankList.txt', '\t'):
        print len(row)
    '''

    #print re.sub('[^A-z0-9_]', '_', 'jeff_AllPermitsWithCodes_(1)')
    # file_info = os.stat(r'Z:\Eris\Documentation\ERISDirect\TestSearches_EDR\OSHA\2016_Sep23\Supporting Files_OSHA\osha_strategic_codes.csv')
    # file_size = file_info.st_size
    # print file_size/(float(1024)**2)

    # file = open('HMIR2014MAR16_part.txt', 'rb')
    # for line in file:
    #     line.decode('utf-8')
    # file.close()


    # file = codecs.open('HMIR2014MAR16_part.txt', encoding = 'utf-8', mode = 'rb')
    # for line in file:
    #     line
            
    # file.close()