
from socket import *
import threading
import hashlib
import time 
import random


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

    

clientdatasize = ['0']*n
countofchunks = [0]*n
clientACKinit = ['0']*n

def BroadcastData(port, i):
    
    clientSocketTCP = socket(AF_INET, SOCK_STREAM)
    clientSocketTCP.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    clientSocketTCP.bind(('', port))
    clientSocketTCP.listen(1)
    connectSock, addr = clientSocketTCP.accept()
    clientrecvdata = ""
    
    for j in range(100):
        clientrecvdata += connectSock.recv(1024).decode('utf-8', 'ignore')
        ack='1'
        # if(j<row):
            # connectSock.send(ack.encode())
    clientSocketTCP.close()
    
    k = i//2 #client number
    clientACKinit[k]=ack
    
    ackSockcl = socket(AF_INET, SOCK_STREAM)
    ackSockcl.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    ackSockcl.bind(('',port+1))
    ackSockcl.connect(('',port+10001))
    ackSockcl.send(ack.encode())
    ackSockcl.close()
    clientdatasize[k]=clientrecvdata[0:3]
    idcount = 10
    chunkid = ''
    count = 0
    chnk = ''
    y=3
    while(y<len(clientrecvdata)):
        if(clientrecvdata[y]=='0'):
            if count != 0:
                clientDataChunk[i//2].append(chnk)  
            cid = ''
            for x in range(y,y+10):
                cid+=clientrecvdata[x]
            chunkIDs[k].append(cid)
            
            y+=10
            chnk = ''
            
        else:
            count +=1
            chnk+=clientrecvdata[y]
            y+=1
    clientDataChunk[i//2].append(chnk)  
    for x in range(len(clientDataChunk[k])):  
        clientCheck[k][int(chunkIDs[k][x])-1]=1 #keeps a check of the chunks recieved
        client_database[k][int(chunkIDs[k][x])-1]=clientDataChunk[k][x]
        chunk = clientDataChunk[k][x]
        if(int(chunkIDs[k][x])==115):
            chunk.replace('#','')
        countofchunks[k]+=1
        fwrite = open(f'./client{k+1}/chunk{int(chunkIDs[k][x])}.txt','w')
        fwrite.write(chunk)
        fwrite.close()
    # print(f"Client ID: {k+1} recieved the following chunks with ids:\n{chunkIDs[k]}\n")
    
    # print(f'Client {k+1} recieved data: \n {clientDataChunk[k]}\n\n\n')
    # print("Recieved from server's port: ", addr," to client number ", k+1, " with port => ", port)
    
listofthreads =[]

for i in range(0, 2*n, 2):
    thread = threading.Thread(target=BroadcastData, args=(clientports[i], i))
    listofthreads.append(thread)
    thread.start()
    

for threads in listofthreads:
    threads.join()

time.sleep(0.1) 


print('Total chunks each client needs to recieve: ')
print(clientdatasize)
print('Count of chunks recieved by each chunk yet: ')
print(countofchunks)



for i in range(n):
    f=0
    listids = []
    listchunks = []
    for j in range(len(clientCheck[i])):
        listids.append(j)
        if(clientCheck[i][j]==1):
            listchunks.append(clientDataChunk[i][f])
            f+=1
        else:
            listchunks.append('')     
    clientdict = dict(zip(listids,listchunks))
    client_database.append(clientdict)
# print(client_database[1])


#define new ports for clients:
client_req_ackports = [] #start from 50000
client_bcast_recports = [] #start from 25000
client_data_sendports = [] #start from 30000


for i in range(n):
    client_req_ackports.append(42590+i)
    client_bcast_recports.append(18000+i)
    client_data_sendports.append(49800+i)
    
#define new ports for servers:
serv_req_ackports = [] #start from 35000
serv_bcast_recports = [] #start from 40000
serv_data_sendports= [] #start from 45000
    
for i in range(n):
    serv_req_ackports.append(35000+i)
    serv_bcast_recports.append(48000+i)
    serv_data_sendports.append(45870+i)
    
#defining queues for data and requests handling
Qack = [0]*n
Qconfirm = [0]*n
Qdatareq = ['0']*n 
Qalldatarec = [0]*n
Qdatanotrec = [95]*n
Qallgot = [True]*n

init = 0

#Requests random server's ports for the files it doesnt have
def sendUDPreq(port,x,init): #client = x+1
    clientSocket = socket(AF_INET, SOCK_DGRAM)
    clientSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    clientSocket.bind(('',port))
    # for i in range(len(clientCheck[x])):
    fwrite = open(f'./client{x+1}RTT.txt','a')
    fwrite.write(f'Time start: {time.time()} \n')
    fwrite.close()
    remain = 92
    while(remain>0):
        for r in range(n):
            fw = open(f'./client{r+1}md5.txt','r')
            fw.write(md5hash.hexdigest())
            fw.close()
        b = True
        for i in range(115):
            if(clientCheck[x][i]!=1):
                b = False
                # print(f"Chunk {i+1} not present with client {x+1}\n Request sent")
                # Qack[x]=0 #debugging
                while(Qack[x]==0):
                    try:
                        message = adjuststring(i+1)+adjuststring(x+1)
                        serverport = 35000+random.randint(0,n) 
                        # print(f'requesting server port {serverport}')             
                        clientSocket.sendto(message.encode(),('', serverport))
                        fwrite = open(f'./client{x+1}RTT.txt','a')
                        fr = open('A2_small_file.txt','r')
                        stri = fr.read()
                        fwrite.write(f'Time of sent for chunk {i+1}: {time.time()} \n')
                        fwrite.close()
                        Qack[x]=1
                        # print('Queue of acks: ',Qack)
                    except ConnectionError:
                        # print("Server's port busy...Trying to connect to another port")
                        Qack[x]=0
                    time.sleep(2)
                fwrite = open(f'./client{x+1}RTT.txt','a')
                fwrite.write(f'Acknowledgement recieved for chunk {i+1}: {time.time()} \n')
                fwrite.close()
                Qack[x]=0               
        remain = 0
        for i in range(115):
            if(clientCheck[x][i]!=1):
                remain+=1
        time.sleep(remain*6)
        md5hash = hashlib.md5(stri.encode())
        print(f'WAITING FOR {remain*6} SECONDS AND SENDING REQUESTS AGAIN')
        
        # init+=1
    clientSocket.close()
    str1 = ''
    for q in range(115):
        fwrite = open(f'./client{x+1}/chunk{q+1}.txt','r')
        str1+=fwrite.read()
        fwrite.close()
    md5hash = hashlib.md5(stri.encode())
    
    # print(f"Client {x+1} recieved all files. MD5 of the file is {md5hash.hexdigest()}")
    # time.sleep(1000)
        
    
    # confirmsent = False
    # while(Qconfirm[x]==0):
    #     try:
    #         mes = f'DONE{x+1}'
    #         serverport = 35000+random.randint(0,n)
    #         clientSocket.sendto(mes.encode(), ('', serverport))
    #         # print(f'Client {x+1} sent request for all unavailable chunk to port {serverport}')
    #         confirmsent=True
                        
    #     except ConnectionError:
    #         # print("Trying to send confirmation to another port...")
    #         confirmsent=False
    #     time.sleep(0.1)
        
    # print(f'Thread sending req {x+1} closing')
    # return 
   
    
                
    # clientSocket.close()

#Recieved acknowledgement from server for recieving client requests
def recackTCP(port,x):
    # try:
        clientSocketTCP = socket()
        clientSocketTCP.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        clientSocketTCP.bind(('', port))
        clientSocketTCP.listen(1)
        allsent = True
        while(allsent):
            connectSock, addr = clientSocketTCP.accept()
            data = connectSock.recv(1024).decode('utf-8','ignore')
            # if(data[0:3]=='ALL'):
            #     print(f'Client {x+1} sent request for all unavailable chunk. Recieved {data} for the same...')
            #     Qconfirm[x]=1
            
            # print(f"Recieved {data} for request of client-{x+1}")
            Qack[x]=1
            connectSock.close()
            k = 1
            # for w in range(n):
            #     if(Qconfirm[w]==0):
            #         k=0
            # if(k==1):
            #     allsent=False
                # print(f'Thread recack {x+1} closing')
                # return 
        # print(f'Thread recack {x+1} closing')
        # return 
            
    # except Exception:
    #     print(f"Failed to reach, sending again...excewption {Exception}")
        
        
        
# sendUDPreq(client_req_ackports[0],0)
lot2=[]

           
for i in range(0,n,1):
    td = threading.Thread(target=sendUDPreq, args=(client_req_ackports[i],i,0))
    lot2.append(td)
    td.start()
    
for i in range(0,n,1):
    td2 = threading.Thread(target=recackTCP, args=(client_req_ackports[i],i))
    lot2.append(td2)
    td2.start()
   
# for t in lot2:
    
#     t.join()

# time.sleep(0.1)

# print("All clients have sent the requests for chunks, now start recieving....")

def recbcast(port,x):
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(('',port))
    while True:
        data, addr = sock.recvfrom(1024)
        data = data.decode('utf-8','ignore')
        chunkreq = data[0:10]
        clientwhichreq = data[10:20]
        print(f'broadcast recieved for chunk{int(chunkreq)} in client {x+1}')

        
        if(clientCheck[x][int(chunkreq)-1]==1):
            # print(f'Data present...sending ACK with data')
            Qdatareq[x] = 'ACK'+client_database[x][int(chunkreq)-1]
            # print('Data sent: ',client_database[x][int(chunkreq)-1])
        #     sock.sendto('NACK'.encode(), addr)
        else:
            # print(f'Data absent, sending NACK')
            Qdatareq[x] = 'NACK'
        #     senddata = 'ACK'+client_database[x][int(chunkreq)-1]
        #     sock.sendto(senddata.encode(), addr)
            
        
def respbcast(port,x):
    # notsent = True
    # print(f'trying to send ack from port {port} to {serv_bcast_recports[x]}')
    while True:
        if(Qdatareq[x]!='0'):
            try: 
                sock = socket(AF_INET, SOCK_STREAM)
                sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                sock.bind(('',port))
                datasend=Qdatareq[x]
                # print(f'trying to send ack from port {port} to {serv_bcast_recports[x]}')
                sock.connect(('',serv_bcast_recports[x]+6))
                sock.send(datasend.encode())
                notsent=False
                # print(f'sent {datasend} to {serv_bcast_recports[x]}')
                Qdatareq[x] = '0'
                sock.close()
            except ConnectionError:
                sock.close()
            except OSError:
                print("Trying connecting again...")
                # print("Not able to send data to server")
            
def recdatafinal(port,x):
    clientSocketTCP = socket(AF_INET, SOCK_STREAM)
    clientSocketTCP.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    clientSocketTCP.bind(('', port+132))
    clientSocketTCP.listen(1)
    while True:
        conn, addr = clientSocketTCP.accept()
        data = conn.recv(1024).decode('utf-8','ignore')
        chid = int(data[0:10])
        chunk = data[10:]
        if(chid==115):
            chunk=chunk.replace('#','')
        print(f'recieved data of chunk {chid} for client {x+1}')
        fwrite = open(f'./client{x+1}/chunk{chid}.txt','w')
        fwrite.write(chunk)
        fwrite.close()
        client_database[x][chid-1]=chunk
        clientCheck[x][chid-1]=1
        Qdatanotrec[x]-=1
        if(Qdatanotrec[x]==0):
            print(f"All data recieved for client {x+1}")
            return       
    
    
  
lot3=[]       

# remain = 92  
 
for i in range(0,n,1):
    thread = threading.Thread(target=recbcast, args=(client_bcast_recports[i],i))
    lot3.append(thread)
    thread.start()
    
for i in range(0,n,1):
    thread = threading.Thread(target=respbcast, args=(client_bcast_recports[i],i))
    lot3.append(thread)
    thread.start()  
 
for i in range(0,n,1):
    thread = threading.Thread(target=recdatafinal, args=(client_data_sendports[i],i))
    lot3.append(thread)
    thread.start()  
    
for t in lot3 :
    t.join()
    
print('Transaction complete')    


