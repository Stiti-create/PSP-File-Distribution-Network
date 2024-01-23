from socket import *
import threading
import hashlib
import time 
import random
import numpy



n = 5  # no. of clients

def adjuststring(x):
    s = str(x)
    l = len(s)
    extras = ""
    for j in range(10, l, -1):
        extras+='0'
    extras+=s
    return extras 

# assign 2n port numbers to server
serverports = []
p1 = 15000
for ports in range(p1, p1+2*n, 1):
    serverports.append(ports)

totalchunks = 115
clientports = []
port1 = 5000

for port in range(5000, 5000+2*n, 1):
    clientports.append(port)

client_database=[]
for i in range(n):
    arr=['']*115
    client_database.append(arr)
clientCheck = []
for i in range(5):
    arr = [0]*115
    clientCheck.append(arr)

TCPclients = []
UDPclients = []
col = n

row = totalchunks//col
clientDataChunk = []  # to store the chunks
chunkIDs = []  # to store the IDs of the chunks, and accordingly request the remaining ones
chunkDatas = []  # to store the data part of the chunks

for j in range(n):
    chunkIDs.append([])
    chunkDatas.append([])
    clientDataChunk.append([])

Qdatarec = [0]*n

clientdatasize = ['0']*n
countofchunks = [0]*n
clientACKinit = ['0']*n
statsrec=[0]*n
allrec=[True]*n

def filerecack(port,x):
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.settimeout(1)
    sock.bind(('',port)) 
    try:
        while(allrec[x]):
            temp=''
            da, addr = sock.recvfrom(1024) 
            data = da.decode('utf-8','ignore')
            if(data!=temp):
                temp=data
                if(data=='DONE'):
                    allrec[x]=False
                    print(f"Client {x} recieved all data chunks assigned")
                    statsrec[x]=0
                else:
                    clientDataChunk[x]+= data
                    clientCheck[x][int(data[:10])-1]=1
                    client_database[x][int(data[:10])-1]=data[10:]
                    fwrite = open(f'./client{x+1}/chunk{int(data[:10])}.txt','w')
                    fwrite.write(data[10:])
                    fwrite.close()
                    statsrec[x]=1
    except:
        allrec[x]=False
    
    
    sock.close()
    return

def TCPsendack (port,x):
    sock2 = socket(AF_INET, SOCK_STREAM)
    sock2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock2.bind(('',port))
    sock2.connect(('',serverports[x]))
    while(allrec[x]):
        try:
            if(allrec[x]==False):
                allsent=False
            if(statsrec[x]==1):  
                ack = "ACK"
                
                sock2.send(ack.encode())
                statsrec[x]=0
                sock2.close()

        except OSError:
            allsent=True
        except ConnectionError:
            allsent=True
        except ConnectionResetError:
            allsent = True
            
    return
ll = []
        
for i in range(n):
    thread = threading.Thread(target=filerecack, args=(clientports[i],i))
    ll.append(thread)
    thread.start()
    
for j in range(n):
    thread = threading.Thread(target=TCPsendack, args=(clientports[j],j))
    ll.append(thread)
    thread.start()
for t in ll:
    t.join()
    
print("Server broadcast done...clients received\nClients start requesting server for missing chunks")     
        
clientreqports=[]
servreqrecports=[]  
clientfinrecports=[] 
clientbcastports=[]
servbcastports=[]
servdatarecports=[]
clientdatasendports=[]
serverclientFIN = 42954
for i in range(n):
    clientreqports.append(35600+i)
    servreqrecports.append(36600+i)
    clientfinrecports.append(39877+i)
    clientbcastports.append(35426+i)
    clientdatasendports.append(36586+i)
    servbcastports.append(35524+i)
    servdatarecports.append(49075+i)
    
def sendmissreq(port,x):
    print(f'Client {x+1} starts sending requests')
    try:
        
        for f in range(totalchunks):
            if(clientCheck[x][f]==0):
                notsent=True
                while(notsent):
                    sock2 = socket(AF_INET, SOCK_STREAM)
                    sock2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                    sock2.bind(('',port))
                    sock2.connect(('',servreqrecports[x]))
                    data = adjuststring(f+1)+adjuststring(x+1)
                    sock2.send(data.encode())
                    sock2.close() 
                    time.sleep(0.1)
                    notsent=False
        sock2 = socket(AF_INET, SOCK_STREAM)
        sock2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        sock2.bind(('',port))
        sock2.connect(('',servreqrecports[x]))
        data = "DONE"
        sock2.send(data.encode())
        print(f'Client {x+1} sent all requests')
        sock2.close() 
        return 
    except:
        notsent=True
    
        
list1=[]
alldone = False
        
for i in range(n):
    thread = threading.Thread(target=sendmissreq, args=(clientreqports[i],i))
    list1.append(thread)
    thread.start()      
    
for t in list1:
    t.join()

bcastq = []
bcastdatarec = [0]*n
finaldatarec = [0]*n
for i in range(n):
    bcastq.append('')
    
def recbroadcast(port,x):
    sock2 = socket(AF_INET, SOCK_STREAM)
    sock2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock2.bind(('',port))
    sock2.settimeout(1)
    alldone=False
    while(not(alldone)):
        sock2.listen(1)
        try:
            s2, addr = sock2.accept()
            data = s2.recv(1024).decode('utf-8','ignore')
            if(data!=''):
                bcastq[x]=data
                print(f'Broadcast recieved for chunk{int(data[:10])} in client {x+1}')
        except:
            alldone=False
    sock2.close()
    return 
   
notpres =[0]*n
def sendDataUDP(port,x):
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(('',port)) 
    while(not(alldone)):
        
        if(bcastq[x]!=''):
            datareq = int(bcastq[x][:10])
            bcastq[x]=''
            if(clientCheck[x][datareq-1]!=0):
                notpres[x]=1
                while(bcastdatarec[x]==0):
                    data = adjuststring(datareq)+client_database[x][datareq-1]
                    sock.sendto(data.encode(),('',servdatarecports[x]))
                    print(f'respond data sent to server for broadcast of chunk {datareq}')
                    time.sleep(0.5)
                bcastdatarec[x]=0 
            else:
                while(bcastdatarec[x]==0):
                    data = 'NACK'
                    sock.sendto(data.encode(),('',servdatarecports[x]))
                    print(f'NACK sent to server for broadcast of chunk {datareq}')
                    time.sleep(0.5)
                bcastdatarec[x]=0 
                notpres[x]=0
                           
    sock.close()
    return

def recackdata(port,x):
    try:
        it = True
        while(it):
            sock = socket(AF_INET, SOCK_STREAM)
            sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            sock.settimeout(1)
            sock.bind(('',port))
            while(not(alldone)):
                sock.listen(1)
                s2, addr = sock.accept()
                ack = s2.recv(1024).decode('utf-8','ignore')
                print(f'ACK recieved from server for client {x+1}')
                if(ack=='ACK'):
                    bcastdatarec[x]=1
                    # it = False            
            sock.close()
            return 
    except:
        it = True
    
  
def recDataUDP(port,x):
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.settimeout(1)
    sock.bind(('',port))
    alldone=False
    while(not(alldone)):
        temp = ''
        try:
            dat, addr = sock.recvfrom(1024)
            data = dat.decode('utf-8','ignore')
            if(data!=temp):
                temp = data
                dataid = int(data[:10])
                datachunk = data[10:]
                print(f'DATA RECIEVED: {data}')
                client_database[x][dataid-1]=datachunk
                fwrite = open(f'./client{x+1}/chunk{dataid}.txt','w')
                fwrite.write(datachunk)
                fwrite.close()
                clientCheck[x][dataid-1]=1
                finaldatarec[x]=1
        except:
            alldone=False
            # print(f'Data of chunk {int(data[:10])} recieved in client {x+1}')
    sock.close()
    return 
            
            
    pass
def recAckfinal(port,x):
    try:
        while(not(alldone)):
            if(finaldatarec[x]==1):
                sock2 = socket(AF_INET, SOCK_STREAM)
                sock2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                sock2.bind(('',port))
                sock2.connect(('',serverclientFIN))
                data = 'ACK'
                sock2.send(data.encode())
                print(f'DATA RECIEVED THANKS')
                finaldatarec[x]=0
                # time.sleep(0.1)
                sock2.close()
        return
    except:
        sent = False   

for i in range(n):
    thread = threading.Thread(target=recbroadcast, args=(clientbcastports[i],i))
    thread.start()
    
for i in range(n):
    thread = threading.Thread(target=sendDataUDP, args=(clientdatasendports[i],i))
    thread.start()

for v in range(n):
    thread = threading.Thread(target=recackdata, args=(clientdatasendports[v],v))
    thread.start()
    
for r in range(n):
    thread = threading.Thread(target=recDataUDP, args=(clientfinrecports[r],r))
    thread.start()
    
for s in range(n):
    thread = threading.Thread(target=recAckfinal, args=(clientfinrecports[s],s))
    thread.start()