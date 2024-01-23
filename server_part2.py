# from concurrent.futures import thread
from socket import *
import random
import threading
import time



#read text file and count number of bytes
text_file = open("A2_small_file.txt", "r")
text = text_file.read()
count = 0
for ch in text:
    count += 1

#add extra bytes of characters to make it equal chunks
rem = count%1000
extrabits = 1000-rem
for j in range(extrabits):
    text+= '#'
    # count+=1
    
#make every chunk-id of 10 bits to ease its concatenation with sending bytes   
def adjuststring(x):
    s = str(x)
    l = len(s)
    extras = ""
    for j in range(10, l, -1):
        extras+='0'
    extras+=s
    return extras 
    
#divide the text into chunks, assign them ids and make them separate strings
chunkid1 = 0
clientchunks = []
clientchunkids = []
for i in range(0, count, 1000):
    chunkid1 = chunkid1 + 1
    clientchunks.append(text[i:i+1000])
    clientchunkids.append(chunkid1)
chunkstosend = []
for j in range(len(clientchunks)):
    chunk = adjuststring(clientchunkids[j])+clientchunks[j]
    chunkstosend.append(chunk)
    
# Now the number of chunks divided is sent equally among n clients
n = 5  # no. of clients
clientfilecount = len(clientchunkids)//n

#assign 2n port numbers to server
serverports = []
p1 = 15000
for ports in range(p1, p1+2*n, 1):
    serverports.append(ports)
print(serverports)
#client ports
clientports = []
port1 = 5000
for port in range(5000, 5000+2*n, 1):
    clientports.append(port)


#divide the chunks among the clients in round robin
col = n
row = len(clientchunkids)//n
# print(row)
# print(len(chunkstosend))
# print(col)
clientdist = []
for i in range(0,len(clientchunkids),n):
    rows = []
    for j in range(i,i+n,1):
        rows.append(chunkstosend[j])
    clientdist.append(rows)
    
#initial queue
Qdatarec = [0]*n
Qallsent = [0]*n
    
def filesendinit(port,x):
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(('',port))
    for l in range(row):
        data = clientdist[l][x]
        while(Qdatarec[x]==0):
            sock.sendto(data.encode(),('',clientports[x]))
            time.sleep(0.1)
        # print(f'Chunk {data[:10]} sent to client {x}')
        Qdatarec[x]=0
    Qallsent[x]=1
    print('Server sent all data to clients')
    sock.sendto('DONE'.encode(), ('',clientports[x]))
    sock.close()
    return

  
def fileackrec(port,x):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(('',port))
    sock.listen(1)
    s2, addr = sock.accept()
    print(addr)
    # all = True
    while(Qallsent[x]==0):        
        ack = s2.recv(1024).decode('utf-8','ignore')
        Qdatarec[x]=1
    print(f'Server sent all files to client {x+1}')
    sock.close() 
    return      


listofthreads =[]
listofthreads2 = []

for i in range(n):
    thread = threading.Thread(target=filesendinit, args=(serverports[i],i))
    listofthreads.append(thread)
    thread.start()

for j in range(n):
    thread = threading.Thread(target=fileackrec, args=(serverports[j],j))
    listofthreads2.append(thread)
    thread.start()
    
for t in listofthreads:
    t.join()

for t2 in listofthreads2:
    t2.join()
    
    
print('server broadcast over...')

clientreqports=[]
servreqrecports=[]
Qportreqs = []   
clientfinrecports=[] 
clientbcastports=[]
servbcastports=[]
servdatarecports=[]
servdatasendackports=[]
clientdatasendports=[]
serverclientFIN = 42954
for i in range(n):
    clientreqports.append(35600+i)
    servreqrecports.append(36600+i)
    Qportreqs.append([])
    clientfinrecports.append(39877+i)
    clientbcastports.append(35426+i)
    clientdatasendports.append(36586+i)
    servbcastports.append(35524+i)
    servdatasendackports.append(46666+i)
    servdatarecports.append(49075+i)
    
def recvreq(port,x):
    sock2 = socket(AF_INET, SOCK_STREAM)
    sock2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock2.settimeout(1)
    sock2.bind(('',port))    
    allrec=False
    while(not(allrec)):
        sock2.listen(1)
        try:
            s2, addr = sock2.accept()
            data = s2.recv(20).decode('utf-8', 'ignore')
            if(data=='DONE'):
                allrec=True
            else:
                Qportreqs[x].append(data)
        except:
            allrec=False
            
    sock2.close()
    print(f'Recieved all requests from client {x+1}')
    
list1 = []    
for i in range(n):
    thread = threading.Thread(target=recvreq, args=(servreqrecports[i],i))
    list1.append(thread)
    thread.start()
    
for t in list1:
    t.join()
    
PendingReq = []
for j in range(len(Qportreqs[0])):
    for i in range(n):
        PendingReq.append(Qportreqs[i][j])
#Pending requests in the form of data as dataid+client who needs it   
print(PendingReq)

def cachehitmiss(arr,dataid):
    for w in range(len(arr)):
        if(arr[w][1]==dataid):
            arr[w]=(time.time(), arr[w][1], arr[w][2])
            return w
    return -1

#in case of cache miss, add to cache
def addtocache(arr,data,dataid,n):
    if (len(arr)<n):
        if(len(arr)==0):
            arr.append((time.time(),dataid,data))
        else:
            arr.insert(1,(time.time(),dataid,data)) 
    else:   
        arr[0]=(time.time(),dataid,data)
    arr.sort()
    return 
        
datasendport = 45365
dataackport = 45375
datarecack = [0]*n
globalCache = []
Qrecbcast = ['']*n
Qsendbcast = ['']*n
Qnomorebcast = [False]*n
bcastrecack=['']*n
alldone = False
#in the form of tuple: (last access time, data id, data)

def broadcastTCP(port,x):
    try:
        sock2 = socket(AF_INET, SOCK_STREAM)
        sock2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        sock2.bind(('',port))
        sent = False
        while(not(sent)):
            sock2.connect(('',clientbcastports[x]))
            if(Qsendbcast[x]!=''):
                sock2.send(Qsendbcast[x].encode())
                print(f'Broadcasted for chunk {Qsendbcast} from port{x+1}')
                Qsendbcast[x]=''
                sent = True
                time.sleep(0.1)
        sock2.close()
        return
    except:
        sent = False
  
recdataq =[0]*n
recdata = 0
def recdataUDP(port,x):
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.settimeout(1)
    sock.bind(('',port))
    rec = False
    while(recdataq[x]==0):
        temp = ''
        try:
            dat, addr = sock.recvfrom(1024)
            data = dat.decode('utf-8','ignore')
            if(data!=temp):
                rec = True
                temp=data
                if(data=='NACK'):
                    recdataq[x]=1
                    print(f'{data} recieved from client {x+1}')                
                else:
                    if(data!=''):
                        Qrecbcast[x]=data
                        print(f'Received required data id {data[:10]} from client {x+1}')
                        recdataq[x]=1
                bcastrecack[x]='ACK' 
        except:
            rec = False                                   
    sock.close()
    return                  

def sendackfordatareq(port, x):
    lock = True
    while(lock):
        try:
            acksent = False
            while(not(acksent)):
                if(recdataq[x]!=0):
                    sock2 = socket(AF_INET, SOCK_STREAM)
                    sock2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                    sock2.bind(('',port))
                    sock2.connect(('',clientdatasendports[x]))
                    # print(f'Trying to send ack to client {x+1}')
                    data = 'ACK'
                    sock2.send(data.encode())
                    # print(f'Trying to send ack to client {x+1}')
                    print(f'sent ACK for data to client {x+1}')
                    acksent=True
                    bcastrecack[x]=''
                    recdataq[x]=0
                    lock=False                   
                    # time.sleep(0.05)
                    sock2.close()        
            return
        except:
            lock = True

   

o=0
while(len(PendingReq)>0):
    o+=1
    print(f'PROCESSING REQUEST {o}')
    dataid = PendingReq[0][:10]
    clientwhoreq = int(PendingReq[0][10:20])
    bcastsend = PendingReq[0]
    PendingReq.pop(0)
    ind = cachehitmiss(globalCache,dataid)
    if (ind==-1):
        #cachemiss
        bcastrec=False   
        print('CACHE MISS, BROADCASTING CHUNK FROM CLIENTS')
        for e in range(n):
            Qsendbcast[e]=bcastsend
        anyrec = False
        lot = []
        for i in range(n):
            thread = threading.Thread(target=broadcastTCP, args=(servbcastports[i],i))
            lot.append(thread)
            thread.start()
            
        for j in range(n):
            thread2 = threading.Thread(target=recdataUDP, args=(servdatarecports[j],j))
            lot.append(thread2)
            thread2.start()

        for rr in range(n):
            thread3 = threading.Thread(target=sendackfordatareq, args=(servdatasendackports[rr],rr))
            lot.append(thread3)
            thread3.start()
            
        for t in lot:
            t.join()
        print('threads joined')
        for m in range(n):
            if(Qrecbcast[m]!=''):
                chunktosend=Qrecbcast[m]
                Qrecbcast[m]=''
                addtocache(globalCache, chunktosend[10:], chunktosend[:10],n)
                break
            
    else:
        #cachehit
        print('CACHE HIT...SENDING CHUNK TO CLIENT')
        chunktosend = globalCache[ind][1]+globalCache[ind][2]
    print(f'Chunk {int(dataid)} to be sent to client {clientwhoreq}')
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(('',datasendport))
    
    while(datarecack[clientwhoreq-1]==0):
        # print(f'Chunk {int(dataid)} sent to client {clientwhoreq}')
        sock.sendto(chunktosend.encode(),('',clientfinrecports[clientwhoreq-1]))
        sock2 = socket(AF_INET, SOCK_STREAM)
        sock2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        sock2.settimeout(1)
        sock2.bind(('',serverclientFIN))
        sock2.listen(1)
        time.sleep(0.5)
        try:
            s, addr = sock2.accept()
            ack = s.recv(10).decode('utf-8','ignore')
            if(ack=='ACK'):
                datarecack[clientwhoreq-1]=1
                print('ACKNOWLEDGED, CLIENT HAPPY')
        except:
            print('trying again')
        
    datarecack[clientwhoreq-1]=0
    print(f'Chunk finally {int(dataid)} sent to client {int(clientwhoreq)}')
    sock.close()
    
    
print('DATA TRANSFER DONE....')
        


           
      