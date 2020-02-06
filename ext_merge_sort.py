import sys
TOTAL_NUM = 1000
NUM = 100
K = TOTAL_NUM//NUM
# dataArry : minimum num of each files
dataArry = [0 for i in range(0,K)]
# initialize loserTree
loserTree = [-1 for i in range(0,K)]
sortedFile = [0 for i in range(0,K)]

def sortInputFile():
    list = [0 for i in range(0,NUM)]
    # handle k unsort files
    for j in range(1,K+1):
        # input jth unsort file
        input = open("input/unsorted_%s.txt"%j,"r")
        for i in range(0,NUM):
            list[i] = int(input.readline().rstrip())
        input.close()
        # sort jth file
        list.sort()
        #output jth sorted file
        output = open("temp/sorted_%s.txt"%j,"w")
        for i in range(0,NUM):
            output.write("%s\n"%list[i])
        output.close()


# create Loser Tree
def createLoserTree():
    for i in range(K-1, -1,-1):
        adjust(i)


# adjust Loser Tree
def adjust(dummy):
    parent = (dummy + K) // 2
    while parent > 0:
        if(dataArry[dummy] > dataArry[loserTree[parent]] or loserTree[parent] == -1):
            temp = dummy
            dummy = loserTree[parent]
            loserTree[parent] = temp
        parent = parent // 2
    loserTree[0] = dummy


def sort():
    # sort 10 input files, generate 10 sorted files
    sortInputFile()
    # initialize dataArry
    for i in range(0,K):
        sortedFile[i] = open("temp/sorted_%s.txt"%(i+1),"r") 
        dataArry[i] = int(sortedFile[i].readline().rstrip())  

    # create Loser Tree
    createLoserTree()

    # output sorted data
    output = open("output/sorted.txt","w")
    while dataArry[loserTree[0]] != sys.maxsize:
        output.write("%s\n"%dataArry[loserTree[0]])
        item = sortedFile[loserTree[0]].readline().rstrip()
        if item != "":
            dataArry[loserTree[0]] = int(item)
            adjust(loserTree[0])
        else:
            sortedFile[loserTree[0]].close()
            dataArry[loserTree[0]] = sys.maxsize
            adjust(loserTree[0])
            
    output.close()

sort()






