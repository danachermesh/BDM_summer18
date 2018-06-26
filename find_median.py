import sys

if __name__ == '__main__':
    mylist = []
    for filename in sys.argv[1:]:
        with open(filename, 'r') as fi:
            for line in fi:
                mylist.append(int(line.strip()))
    mylist.sort()
    print(mylist[int(len(mylist)/2)])
