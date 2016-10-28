

if __name__ == '__main__':
    str1 = 'street name'
    str2 = 'street_name_listagg'
    
    int_matched_length = 0
    short_length = len(str1)
    long_length = len(str2)
    
    step = len(str1)
    while step > 0:
        for i in range(short_length-step+1):
            str1_seg = str1[i:i+step]
            for j in range(len(str2)-step+1):
                str2_seg = str2[j:j+step]
                if str1_seg == str2_seg:
                    str1 = str1[0:i] + str1[i+step:len(str1)]
                    str2 = str2[0:j] + str2[j+step:len(str2)]
                    int_matched_length += len(str1_seg)
                    print str1_seg + '\t' + str2
                    print int_matched_length
                    
                    
        step -= 1
     
    
    print int_matched_length/float(long_length)
