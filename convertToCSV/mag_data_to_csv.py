input_file = open('mf.2021.txt', 'r')
output_file = open('mf.2021.csv', 'w')

input_file.readline()
input_file.readline()
input_file.readline()
input_file.readline()
input_file.readline()
input_file.readline()
for line in input_file:
    # there are two cases:
    new_line = line.strip().split(' ')
    # new_line = line.strip().split('\t')
    if (new_line == ['']):
        continue
    for (index,item) in enumerate(new_line):
        new_line[index] = new_line[index].strip()
        if (new_line[0][0]=='0'):
            new_line[index]=new_line[0][1]
        if (new_line[index]==''):
            continue
        else:
            output_file.write(new_line[index])
            output_file.write(',')
        
    output_file.write('\n')
    # print(new_line)
input_file.close()
output_file.close()