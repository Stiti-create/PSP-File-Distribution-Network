
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


# def broadcast(chunkstosend):
serverTCPsockets=[]
serverUDPsockets=[]
    

# print("The server is ready to receive\n")
wholeMD5 = '9f9d1c257fe1733f6095a8336372616e'

clientACKsinit = ['0']*n

#function for the initial data sending by server
def broadCastTCP(port,x):
    clientPort = port-10000
    SocketTCP = socket(AF_INET, SOCK_STREAM)
    SocketTCP.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    SocketTCP.bind(('',port))
    SocketTCP.connect(('',clientPort))
    clientno=x//2+1
    # print(f'From Server port:{port} sending to Client number {clientno} with port:{clientPort}')
    SocketTCP.send('115'.encode())
    for i in range(row):
        data = clientdist[i][x//2]
        SocketTCP.send(data.encode())
        # ack = '1'
        # ack = SocketTCP.recv(1024).decode('utf-8', 'ignore')
    # clientACKsinit[clientno-1]= ack
    SocketTCP.close() 
    
    acksockserv = socket(AF_INET, SOCK_STREAM)
    acksockserv.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    acksockserv.bind(('', port+1))
    acksockserv.listen(1)
    ackSockcl, addr = acksockserv.accept()
    clientACKsinit[clientno-1]=ackSockcl.recv(1024).decode('utf-8','ignore')
    acksockserv.close()
    
    if(clientACKsinit[clientno-1]=='1'):
        # print(f'Received ACK=1 from Client {clientno}')
        for i in range(row):
            clientdist[i][clientno-1]=''
        # print(f'Files sent to client {clientno} deleted from the server database')            
    else:
        print(f'Received NACK from Client {clientno}')
        ##Program to resend
        # SocketTCP.close()
        
    

#using multithreading for handling requests:
# print("\nServer Broadcast starts...\n")
listofthreads =[]

for i in range(0,2*n,2):
    thread = threading.Thread(target=broadCastTCP, args=(serverports[i],i))
    listofthreads.append(thread)
    thread.start()

for threads in listofthreads:
    threads.join()
    
# print('\nAll files distributed by server\n')

time.sleep(0.1)   

#File distribution and requests by chunks starts
# print('Clients start requesting chunks from the server')



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

#reeuest handling queue:
Qrequests=[]
Hashmapreq = []
for i in range(n):
    Qrequests.append([])
    Hashmapreq.append({})

#data requests queue:
Qdatareq = []
Qdatafetched = [] #for storing the data fetched from the clients or from cache to send
Qbcastack = []

for i in range(n):
    Qrequests.append(0)
    Qbcastack.append(0)
Qfinalacks = ['']*n
Qallreqrec = [0]*n

tempcach = [(0,0)]*n
    
def cachehitmiss(arr,dataid):
    for w in range(len(arr)):
        # if(arr[w]!=[]):
        if(arr[w][1]==dataid):
            arr[w]=(time.time(), arr[w][1], arr[w][2])
                # arr.sort()
            return w
        
            #cachehit
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


#function to listen to data requests from clients and store them in the request handling queue
def handleUDPreq(port, x):
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    serverSocket.bind(('', port))
    allnotrec = True
    while (allnotrec):
        message, clientaddr = serverSocket.recvfrom(1024)
        data = message.decode('utf-8','ignore') 
        if(data[0:4]=='DONE'):
            cl = int(message[4:])
            Qallreqrec[cl-1]=1
            # print(f'Recieved all requests from client {cl}')     
            # print(f'Closing thread {x} ')
            # return  
        else:
                   
            chunkidreq = data[0:10]
            reqfromclient = data[10:20]
            # dup = False
            # # for d in range(n):
            # #     if((chunkidreq, reqfromclient) in Hashmapreq[d]):
            # #         # print(f"Duplicate request recieved for chunk {int(chunkidreq)} from client {int(reqfromclient)}")
            # #         dup = True
            # if(not(dup)):           
            print(f'Server recieved request for chunk {int(chunkidreq)} from client {int(reqfromclient)} on port {port}')
            Qrequests[x].append((chunkidreq,reqfromclient))
        c = 1
        for w in range(n):
            if(Qallreqrec[w]==0):
                c=0
        if(c!=0):
            allnotrec=False
            # print('ALL REQUESTS RECIEVED, CLOSING RESPONSE PORTS')
            # print(f'all not rec: {Qallreqrec}')
            serverSocket.close()
            # print(f'Thread {x+1} closing')
            # return
    # print(f'Thread {x+1} closing')
    # return    
        # print(Qrequests)
    
#function to send acknowledgement to client on recieving its requests      
def sendTCPack(port,x):
    allacksent = False
    while not(allacksent):
        while(len(Qrequests[x])>0):

               socketTCP = socket(AF_INET, SOCK_STREAM)
               socketTCP.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
               socketTCP.bind(('',port))                
               (chunkid,clientid)=Qrequests[x][0]
               Qrequests[x].pop(0)
               Qdatareq.append((chunkid,clientid))
               lock = True
               while lock:
                   try:
                    socketTCP.connect(('',client_req_ackports[int(clientid)-1]))
                    lock = False
                   except OSError:
                       print("Port Busy...Error") 
                   except ConnectionRefusedError:
                       print("ConnectionRefuseError")    
                
               socketTCP.send("ACK".encode())
            #    print(f'sent acknowledgement to client {int(clientid)}')
               socketTCP.close()
               
        
            # except ConnectionRefusedError:
               
        # if(Qallreqrec[x]==1):
        #     # try: 
        #        socketTCP = socket(AF_INET, SOCK_STREAM)
        #        socketTCP.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        #        socketTCP.bind(('',port))  
        #        socketTCP.connect(('',client_req_ackports[x]))
        #        socketTCP.send("ALLACK".encode())
        #        allacksent=True
        #     #    print(f'sent confirmation to client {x+1}')
        #        socketTCP.close()
    # print(f'Thread {x+1} for ack closing')
    # return 
                              
            # except ConnectionError:
            #    print(f"No ack sent to client {int(clientid)}")
 

lot2=[]
lot4 = []
           
for i in range(0,n,1):
    thread = threading.Thread(target=handleUDPreq, args=(serv_req_ackports[i],i))
    lot2.append(thread)
    thread.start()
    
for i in range(0,n,1):
    thread = threading.Thread(target=sendTCPack, args=(serv_req_ackports[i],i))
    lot4.append(thread)
    thread.start()
 
# for i in range(5):
#     lot2[i].join()
#     lot4[i].join()
    
# time.sleep(0.1)

print("Server recieved requests from all chunks, start sending.....\n\n")



# function for checking data 
def dataaskbcast(x, clid, chunkid):
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(('',serv_bcast_recports[x]))
    
    # print(f"Broadcast...... requesting chunk {int(chunkid)+1} from clients")
    if(int(clid)!=x+1):
        Qbcastack[x]=0
        while(Qbcastack[x]==0):#broadcase not recieved
            data = chunkid+clid
            data = data.encode()
            sock.sendto(data, ('',client_bcast_recports[x]))
            # print(f"Broadcast...... requesting chunk {int(chunkid)} from client {x+1}")
            time.sleep(2)
        # print(f'Broadcast recieved, closing socket {x+1}')
        sock.close()
        return
    else:
        # print(f'Client same, closed')
        sock.close()
        return
        # if(Qbcastack[x]==1):#broadcast recieved, data present
        #     print(f'ACK recieved from client {x+1} for chunk {int(chunkid)}')
        #     sockTCP = socket(AF_INET, SOCK_STREAM)
        #     sockTCP.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        #     sockTCP.bind(('',port2))
        #     sockTCP.listen(1)
        #     while(True):
        #         cs, addr = sockTCP.accept()
        #         data = cs.recv(1024).decode()
        #         print(f'data recieved from client {x+1} for chunk {int(chunkid)}')
        #         cs.close()               
             
            
            
            
def recbcastack(x, clid, chunkid):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(('',serv_bcast_recports[x]+6))
    # print(f'Socket {x+1} to recieve bcast acks created')
    sock.listen(1)
    Qbcastack[x]=0
    if(x+1==int(clid)):
        return
    else:
        
        while(Qbcastack[x]==0):
            cs, addr = sock.accept()
            data = cs.recv(1024).decode('utf-8', 'ignore')
            # print(f'data: {data}')
            # Qbcastack[x]=1
            # print(f'Data recieved: {data[0:4]} from client{x+1}')
            if(data[:4]=="NACK"):
                Qbcastack[x]=2
                # print(f'NACK recieved from client {x+1}')
            else:
                # if(data[:3]=="ACK"):
                Qbcastack[x]=1
                print(f'ACK and data recieved from client {x+1}')
                tempcach[x]=(chunkid, data[3:])
                # addtocache(servercacheids, data[3:], chunkid, 5)
            time.sleep(0.1)
            cs.close()
            return 
        sock.close()
        return
    

# t=random.rand(45000,45005)
p = 45070
#cache of the server
servercacheids = []
cachecount = 0

#cache in form of tuples (a,b,c): a=freq, b=id, c=data

while(True):
    while(len(Qdatareq)>0):
        # print(Qdatareq)
    # for datareq in Qdatareq:
        datareq = Qdatareq[0]
        Qdatareq.pop(0)
        chunkreq = datareq[0]
        clienttosend = datareq[1]
        ind = cachehitmiss(servercacheids ,chunkreq)
        # print('cache: ',servercacheids)
        if(ind==-1):
            print('CACHE MISS')
            lot3 = []
            lot4 = []
            print(f'data required: chunk num {chunkreq}')
            for i in range(n):
                t = threading.Thread(target=dataaskbcast, args=(i,clienttosend,chunkreq))
                lot3.append(t)
                t.start()
            for j in range(n):
                tt = threading.Thread(target=recbcastack, args=(j,clienttosend,chunkreq))
                lot4.append(t)
                tt.start()
            time.sleep(2)
            for t1 in lot3:
                t1.join()
            # print(f'Threads done')
            for t2 in lot4:
                t2.join()
                
            for d in range(n):
                if(tempcach[d]!=(0,0)):
                    addtocache(servercacheids, tempcach[d][1], tempcach[d][0], n)
                    tempcach[d]=(0,0)
            ind = cachehitmiss(servercacheids, chunkreq)
                
            # print('Threads joined')
            # # for g in range(len(servercacheids)):
            # found = False
            # print(f'recieved data {chunkreq}')
            # for w in range(len(servercacheids)):
            #     if(servercacheids[w][1]==chunkreq and not(found)):
            #         ind = w
            #         found=True
        else:
            print(f'CACHE HIT: chunk {chunkreq} found in cache database')
            # print(f'Cache: {servercacheids}')
        # print(f'ind: {ind}')  
        notsent=True
        while(notsent):
            try:
                socketTCP = socket(AF_INET, SOCK_STREAM)
                socketTCP.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                socketTCP.bind(('',p))  
                socketTCP.connect(('',client_data_sendports[int(clienttosend)-1]+132))
                # print(servercacheids)
                # if(ind==-1):
                #     ind=0
                print(f'{servercacheids[ind][1]} chunk found to be sent at ind: {ind} of cache')
                data = servercacheids[ind][1]+servercacheids[ind][2]
                socketTCP.send(data.encode())
                # print('data sent....')
                notsent = False
                print(f'sent data {chunkreq} to client {int(clienttosend)}, with chunkid: {data[:10]}')
                # print(data)
                socketTCP.close()
                time.sleep(0.1)
            except ConnectionError:
                print('Client port busy, trying again...')
            except OSError:
                print("Congestion...trying again...") 
    time.sleep(1)
    print("ALL CLIENTS RECIEVED DATA") 
    # print(Qdatareq)

print(f"RTT: {time.time()}")       
    

#function to send data broadcast to clients, recieve data from them and store it in the cache to send it back
    
