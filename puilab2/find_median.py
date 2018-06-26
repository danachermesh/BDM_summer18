import fileinput

if __name__=='__main__':
    myList = []
    for line in fileinput.input():
        number = int(line.strip())
        myList.append(number)
    myList.sort()
    print(myList[int(len(myList)/2)])
