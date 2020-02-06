import sys
import asyncio
import queue
TOTAL_NUM = 1000
NUM = 100
K = TOTAL_NUM//NUM
MAX = sys.maxsize
# dataArry : minimum num of each files
dataArry = [0 for i in range(0,K)]
# initialize loserTreec
loserTree = [-1 for i in range(0,K)]
sortedFile = [0 for i in range(0,K)]
q = queue.Queue()
# output sorted data
output = open("output/sorted.txt","w")

async def sortInputFile(j):
    list = [0 for i in range(0,NUM)]
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


async def write(output):
    while True:
        if not q.empty():
            curr = q.get()
            if curr == MAX:
                break
            else:
                output.write("%s\n"%curr)


async def WriteFromQueue():
    if not q.empty():
        curr = q.get()
        output.write("%s\n"%curr)



async def sort():

    # merge
    while dataArry[loserTree[0]] != MAX:
        if q.qsize() < NUM:
            q.put(dataArry[loserTree[0]])
        else:
            # Memory is full
            await WriteFromQueue()
            q.put(dataArry[loserTree[0]])

        item = sortedFile[loserTree[0]].readline().rstrip()
        if item != "":
            dataArry[loserTree[0]] = int(item)
            adjust(loserTree[0])
        else:
            sortedFile[loserTree[0]].close()
            dataArry[loserTree[0]] = MAX
            adjust(loserTree[0])
    q.put(MAX)        
    


tasks = []
for i in range(0,K):
    tasks.append(asyncio.ensure_future(sortInputFile(i+1)))
loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(tasks))

# initialize dataArry
for i in range(0,K):
    sortedFile[i] = open("temp/sorted_%s.txt"%(i+1),"r") 
    dataArry[i] = int(sortedFile[i].readline().rstrip())  

# create Loser Tree
createLoserTree()
# output sorted data
output = open("output/sorted.txt","w")

multi_tasks = [ asyncio.ensure_future(sort()), asyncio.ensure_future(write(output))]
loop.run_until_complete(asyncio.wait(multi_tasks))

output.close()







