




if __name__ == '__main__':
    file_name = r'Z:\Eris\Documentation\ERISDirect\TestSearches_EDR\OSHA\2016_Sep23\OSHA_Inspections\osha_inspection.csv'
    #file_name = r'Z:\Eris\Documentation\ERISDirect\TestSearches_EDR\OSHA\2016_Sep23\OSHA_Accidents\osha_accident.csv'
    
    # lst1 = ['220063804'
           # , '220222954']
    lst1 = ['16148850','12175428','12783965','13412929','17120338','10215291'
           ,'13828124','218171817','16317521','217963784','12454690','217996842']
    lst = []    
    file = open(file_name, 'r')
    count = 0
    for line in file:
        if count == 0:
            lst.append(line)
        for item in lst1:
            if line.find(item) > -1:
                lst.append(line)
        count += 1 
    file.close()        

    str = ''
    for line in lst:
        str += line
    
    file = open('search_result.txt', 'w')
    file.write(str)
    file.close()    


'''
220063804    
220222954
'''

'''
16148850
12175428
12783965
13412929
17120338
10215291
13828124
218171817
16317521
217963784
12454690
217996842
'''