

if __name__ == '__main__':
    file_path = r'C:\Users\jesong\file_to_load\allspills.txt'

    file = open(file_path, 'r')
    max_length = 0
    max_line = 0
    count = 0
    for line in file:
        count += 1
        length = len(line)
        if max_length < length:
            max_length = length
            max_line = count
    file.close()
    print 'line ', max_line
    print 'length', max_length
