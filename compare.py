
def str_similiarity(str1, str2):
    if len(str1) > len(str2):
        temp = str1
        str1 = str2
        str2 = temp
    
    int_matched_length = 0
    short_length = len(str1)
    long_length = len(str2)
    
    step = len(str1)
    while step > 2:
        for i in range(short_length-step+1):
            str1_seg = str1[i:i+step]
            for j in range(len(str2)-step+1):
                str2_seg = str2[j:j+step]
                if str1_seg == str2_seg:
                    str1 = str1[0:i] + str1[i+step:len(str1)]
                    str2 = str2[0:j] + str2[j+step:len(str2)]
                    int_matched_length += len(str1_seg)
                    
                    
                    
        step -= 1
     
    
    return int_matched_length/float(long_length)

def compare():
    file = open('temp1.txt', 'rU')
    lst1 = []
    for line in file:
        lst1.append(line.replace('\n', ''))
    
    file.close()
    
    
    
    file = open('temp2.txt', 'rU')
    lst2 = []
    for line in file:
        lst2.append(line.replace('\n',''))
    file.close()
    
    if len(lst1) < len(lst2):
        temp = lst1
        lst1 = lst2
        lst2 = temp
    
    
    lst3 = []
    for i in range(len(lst1)):
        ratio_similar = -1
        for j in range(len(lst2)):
            
            if ratio_similar < str_similiarity(lst1[i], lst2[j]):
                ratio_similar = str_similiarity(lst1[i], lst2[j])
                tup = (lst1[i], lst2[j], ratio_similar)

        lst3.append(tup)    
    
    
    for i in range(len(lst3)):
        for j in range(len(lst3)):
            if lst3(i)[1] == lst3(j)[1] and i != j:
                if lst3(i)[2] < lst3(j)[2]:
                    lst3(i) = (lst3(i)[0],None,  None)
                else:
                    lst3(j) = (lst3(j)[0],None,  None)
                    
    
    
    str_output = ''
    
    for tup in lst3:
        for item in tup: 
            str_output += str(item) + '\t'
        str_output += '\n'
    file = open('result.txt', 'w')
    file.write(str_output)
    file.close()
    

    
     
    
    

if __name__ == '__main__':

    compare()
    #print str_similiarity(',TRIM(DEPCONTACT1)','LOCATIONDESCRIPTION')
    