import sys
import json
import socket
import time
import random
import threading
import copy
import numpy as np

requests = []
finishRequests = {}
finishReducer = {}
#toFinishMap = []
jobRequest = {}


countJobs = 0
countJobsLock = threading.Lock()
jobRequestLock = threading.Lock()
finishRequestsLock = threading.Lock()
finishReducerLock = threading.Lock()

configPath = sys.argv[1]
scheduleMethod = sys.argv[2]
f = open(configPath)
configuration = json.loads(f.read())
print(configuration)

requestSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
requestSocket.settimeout(20.0)
requestSocket.bind(("localhost", 5000))
requestSocket.listen(1)

workerSocket1 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
workerSocket1.bind(("localhost", configuration['workers'][0]['port']))
workerSocket1.listen(1)

workerSocket2 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
workerSocket2.bind(("localhost", configuration['workers'][1]['port']))
workerSocket2.listen(1)

workerSocket3 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
workerSocket3.bind(("localhost", configuration['workers'][2]['port']))
workerSocket3.listen(1)


listenUpdateSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
listenUpdateSocket.settimeout(20.0)
listenUpdateSocket.bind(("localhost", 5001))
listenUpdateSocket.listen(3)
#listenUpdateSocket.settimeout(100.0)

jobLogs = {}
jobLogsLock = threading.Lock()
# job_id: time
taskLogs = {}
taskLogsLock = threading.Lock()
# task_id: [time,worker]

currentConfiguration = copy.deepcopy(configuration)
configurationLock = threading.Lock()



def acceptRequest():
	global countJobs
	while 1:
		try:
			conn,addr = requestSocket.accept()
		except:
			break
		u = conn.recv(1024).decode()							# Read task completion info
		update = ""
		while(len(u)!=0):
			update += u
			u = conn.recv(1024).decode()
		data_loaded = json.loads(update)
		conn.close()

		if(data_loaded):
			countJobsLock.acquire()
			countJobs+=1
			countJobsLock.release()
			jobRequestLock.acquire()
			jobRequest = data_loaded
			requests.append(jobRequest)
			jobRequestLock.release()
			jobLogsLock.acquire()
			jobLogs[data_loaded['job_id']] = time.time()
			jobLogsLock.release()
			#print(jobRequest)
			#print(countJobs)



def sendToWorker(chosenTask,workerNumber):
	chosenTask['workerNumber']=workerNumber
	if(workerNumber == 0):
		conn, addr = workerSocket1.accept()
		conn.send((json.dumps(chosenTask)).encode())
		conn.close()
	if(workerNumber == 1):
		conn, addr = workerSocket2.accept()
		conn.send((json.dumps(chosenTask)).encode())
		conn.close()
	if(workerNumber == 2):
		conn, addr = workerSocket3.accept()
		conn.send((json.dumps(chosenTask)).encode())
		conn.close()
	



def randomScheduling(chosenTask):
	numberOfWorkers = len(currentConfiguration['workers'])
	workerNumber = np.random.randint(0,numberOfWorkers)
	configurationLock.acquire()
	while currentConfiguration['workers'][workerNumber]['slots'] == 0:
		configurationLock.release()
		time.sleep(1)
		workerNumber = np.random.randint(0,numberOfWorkers)
		configurationLock.acquire()
	currentConfiguration['workers'][workerNumber]['slots']-=1
	print(currentConfiguration)
	print("\n")
	configurationLock.release()
	#print(chosenTask)
	sendToWorker(chosenTask,workerNumber)
	#print('Task with task id ',chosenTask['task_id'],' is being scheduled on worker with id',currentConfiguration['workers'][workerNumber]['worker_id'])

def roundRobin(chosenTask):
	numberOfWorkers = len(currentConfiguration['workers'])
	workerNumber=0
	workerId = 0
	configurationLock.acquire()
	copyConfig = copy.deepcopy(currentConfiguration['workers'])
	copyConfig.sort(key=lambda x:x['worker_id'])
	workerId = copyConfig[workerNumber]['worker_id']
	while copyConfig[workerNumber]['slots'] == 0:
		configurationLock.release()
		time.sleep(1)
		workerNumber = (workerNumber+1)%numberOfWorkers
		configurationLock.acquire()
		copyConfig = copy.deepcopy(currentConfiguration['workers'])
		copyConfig.sort(key=lambda x:x['worker_id'])
		workerId = copyConfig[workerNumber]['worker_id']
	configurationLock.release()
	#currentConfiguration['workers'][workerNumber]['slots']-=1
	#print(currentConfiguration)
	#print("\n")
	indexNumber=-1
	#minLoadingIndex=currentConfiguration['workers'].find(currentConfiguration['workers']['worker_id']=minLoadingIndex)
	for i in range(0,len(currentConfiguration['workers'])):
		if(currentConfiguration['workers'][i]['worker_id']==workerId):
			indexNumber=i
			break

	configurationLock.acquire()
	currentConfiguration['workers'][indexNumber]['slots']-=1
	print(currentConfiguration)
	#print("\n")
	#print(currentConfiguration['workers'][minLoadingIndex]['slots'],' is slots')
	configurationLock.release()

	#print(chosenTask)
	sendToWorker(chosenTask,indexNumber)
	#print('Task with task id ',chosenTask['task_id'],' is being scheduled on worker with id',currentConfiguration['workers'][workerNumber]['worker_id'])


def leastLoaded(chosenTask):
	numberOfWorkers = len(currentConfiguration['workers'])
	workerNumber=0
	configurationLock.acquire()
	copyConfig = copy.deepcopy(currentConfiguration['workers'])
	copyConfig.sort(key=lambda x:x['slots'],reverse=True)
	minLoadingIndex = copyConfig[0]['worker_id']
	#while currentConfiguration['workers'][workerNumber]['slots'] == 0:
	while copyConfig[0]['slots']==0:
		configurationLock.release()
		time.sleep(1)
		configurationLock.acquire()
		copyConfig = copy.deepcopy(currentConfiguration['workers'])
		copyConfig.sort(key=lambda x:x['slots'],reverse=True)
		minLoadingIndex = copyConfig[0]['worker_id']
		#if minLoading<noSlots and noSlots!=0:
		#minLoading=noSlots
		#	minLoadingIndex=workerNumber 
		#workerNumber = (workerNumber+1)
		#if workerNumber==numberOfWorkers:
		#	break;
		#configurationLock.acquire()
	print(currentConfiguration)
	#print(minLoadingIndex)
	configurationLock.release()
	
	if minLoadingIndex!=-1:
		configurationLock.acquire()
		indexNumber=-1
		#minLoadingIndex=currentConfiguration['workers'].find(currentConfiguration['workers']['worker_id']=minLoadingIndex)
		for i in range(0,len(currentConfiguration['workers'])):
			if(currentConfiguration['workers'][i]['worker_id']==minLoadingIndex):
				indexNumber=i
				break
		currentConfiguration['workers'][indexNumber]['slots']-=1
		#print(currentConfiguration)
		#print("\n")
		#print(currentConfiguration['workers'][minLoadingIndex]['slots'],' is slots')
		configurationLock.release()
		#print(chosenTask)
		sendToWorker(chosenTask,indexNumber)
		#print('Task with task id ',chosenTask['task_id'],' is being scheduled on worker with id',currentConfiguration['workers'][minLoadingIndex]['worker_id'])




def scanSchedule():
	global requests
	while(1):
		freeFlag=False
		configurationLock.acquire()
		for i in currentConfiguration['workers']:
			if i['slots']>0:
				freeFlag=True
				break;
		configurationLock.release()
		if freeFlag:
			# got atleast one worker free 
			# now get task and schedule that task
			chosenTask = {}
			rOver=False
			findTask = False
			for j in requests:
				finishRequestsLock.acquire()
				# mapper left so have to do mapper
				if(len(j['map_tasks'])):
					chosenTask = j['map_tasks'][0]
					#toFinishMap.append(j['map_tasks'][0])
					#finishRequestsLock.acquire()
					if j['job_id'] not in finishRequests.keys():
						finishRequests[j['job_id']]=[]
					finishRequests[j['job_id']].append(chosenTask)
					#finishRequestsLock.release()
					j['map_tasks'] = j['map_tasks'][1:]
					findTask=True
					finishRequestsLock.release()
					break
				#schedule reducers
				elif len(finishRequests[j['job_id']])==0:
					finishRequestsLock.release()
					#print(j,j['reduce_tasks'],' IS J REDUCE TASKS')
					chosenTask = j['reduce_tasks'][0]
					if j['job_id'] not in finishReducer.keys():
						finishReducer[j['job_id']]=[]
					finishReducer[j['job_id']].append(chosenTask)
					j['reduce_tasks'] = j['reduce_tasks'][1:]
					findTask=True
					if(len(j['reduce_tasks'])==0):
					#	jobLogsLock.acquire()
					#	jobLogs[j['job_id']] = time.time()-jobLogs[j['job_id']] 
					#	jobLogsLock.release()
						jobRequestLock.acquire()
						requests.remove(j)
						jobRequestLock.release()
						rOver = True
					break
				else:
					finishRequestsLock.release()
			#if findTask:
			#	finishRequestsLock.release()		
			#if rOver:
				#requests = requests[1:]
			#print('seems free')
			if findTask and scheduleMethod == 'RANDOM':
				randomScheduling(chosenTask)
				pass
			elif findTask and scheduleMethod == 'RR':
				roundRobin(chosenTask)
				pass
			elif findTask and scheduleMethod == 'LL':
				leastLoaded(chosenTask)
				pass

def recieveUpdates():
	while 1:
		try:
			conn, addr = listenUpdateSocket.accept()
		except:
			break
		r = conn.recv(1024)						# Read job request
		req = ""
		while r:							# If len(req) > 1024b
			req += r.decode()
			r = conn.recv(1024)
		data_loaded = json.loads(req)					
		conn.close()
		#print(data_loaded,' is what we recieve from worker')
		configurationLock.acquire()
		currentConfiguration['workers'][data_loaded['workerNumber']]['slots']+=1
		configurationLock.release()
		job_id = data_loaded['task_id'][:data_loaded['task_id'].find('_')]
		curr_task = {}
		curr_task['task_id']=data_loaded['task_id']
		curr_task['duration']=data_loaded['duration']
		curr_task['workerNumber']=data_loaded['workerNumber']
		taskLogsLock.acquire()
		taskLogs[data_loaded['task_id']] = []
		taskLogs[data_loaded['task_id']].append(data_loaded['end_time']-data_loaded['start_time'])
		#configurationLock.acquire()
		taskLogs[data_loaded['task_id']].append(currentConfiguration['workers'][data_loaded['workerNumber']]['worker_id'])
		#configurationLock.acquire()
		taskLogsLock.release()
		if data_loaded['task_id'][data_loaded['task_id'].find('_')+1] == 'M':
			finishRequestsLock.acquire()
			#print(finishRequests[job_id],' is the finish requests list')
			#print(curr_task, ' is the task to remove')
			finishRequests[job_id].remove(curr_task)
			finishRequestsLock.release()
		else:
			finishReducerLock.acquire()
			#print(curr_task)
			#print(finishReducer[job_id])
			finishReducer[job_id].remove(curr_task)
			#print(finishReducer[job_id])
			if(len(finishReducer[job_id])==0):
				finishReducerLock.release()
				jobLogsLock.acquire()
				#print(jobLogs[job_id],' is job_id before')
				jobLogs[job_id] = time.time() - jobLogs[job_id]#data_loaded['end_time'] - jobLogs[job_id]	# Update duration of job
				#print(jobLogs[job_id])
				jobLogsLock.release()
			else:
				finishReducerLock.release()
		#print(taskLogs,' that was TASK_LOG')
		print("\n----*******----")
		print(currentConfiguration,' is current configuration')
		print("----*******----\n")
		#print(data_loaded,' is from worker')

thread1 = threading.Thread(target = acceptRequest)
thread2 = threading.Thread(target = scanSchedule, daemon = True)
thread3 = threading.Thread(target = recieveUpdates)
thread1.start()
thread2.start()
thread3.start()


thread1.join()
thread3.join()
thread2.killed = True

requestSocket.close()
listenUpdateSocket.close()
workerSocket1.close()
workerSocket2.close()
workerSocket3.close()
print("\n\n\n")
print("Task Logs:\n")
print(taskLogs)
print("\n")
print("Job Logs:\n")
print(jobLogs)


if(sys.argv[2] == 'RANDOM'):
	fileName = "random.txt"
elif(sys.argv[2] == 'RR'):
	fileName = "round.txt"
else:
	fileName = "LL.txt"
	
fp = open(fileName, 'w')
fp.write(json.dumps(taskLogs))
fp.write('\n')
fp.write(json.dumps(jobLogs))
fp.close()

exit(0)
