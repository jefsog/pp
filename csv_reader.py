import csv
with open('C:\eris_python\csv\Hazardous_Waste_Sites.csv', 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    #print spamreader
    
    for row in spamreader:
        print row
        print ', '.join(row)
    