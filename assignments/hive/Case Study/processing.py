with open('bankmarketdata.csv','r') as file:
    f = open("bankmarketing.txt",'a+')
    for line in file:
        line = line.replace('"', '')
        values = line.split(' ')
        record = " ".join(values)
        f.write(record)
        f.write("\n")       
f.close()
