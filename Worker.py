import socket
import sys
import json
import threading
import time



# task comes as {task_id,duration}
executionList = []
executionListLock = threading.Lock()

#executionDuration = {}
#executionDurationLock = threading.Lock()
def listenRequest():
	while 1:
		requestSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		requestSocket.connect(("localhost", int(sys.argv[1])))
		data = requestSocket.recv(1024)
		# De-serializing data
		data_loaded = json.loads(data)
		data_loaded['start_time'] = time.time()
		data_loaded['end_time'] = data_loaded['start_time'] +data_loaded['duration']
		#executionDurationLock.acquire()
		#executionDuration[data_loaded['task_id']] = data_loaded['duration']
		#executionDurationLock.release()
		#time.sleep(data_loaded['duration'])
		executionListLock.acquire()
		executionList.append(data_loaded)
		executionListLock.release()
		print(data_loaded)

def executionDetails(task):
	detailsToMaster= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	detailsToMaster.connect(("localhost", 5001))
	#conn, addr = workerSocket3.accept()
	message = json.dumps(task)
	detailsToMaster.send(message.encode())
	detailsToMaster.close()



def executeNotify():
	while 1:
		for task in executionList:
			currentTime = time.time()
			if currentTime>=task['end_time']:
				task['end_time'] = currentTime
				executionListLock.acquire()
				executionList.remove(task)
				executionListLock.release()
				executionDetails(task)
			#executionDurationLock.acquire()
			#executionDuration[task['task_id']] -=1
			#if executionDuration[task['task_id']]==0:
			#	executionDurationLock.release()
			#else:
			#	executionDurationLock.release()
				#task['duration']-=1	
		time.sleep(1)

# listen to request
thread1 = threading.Thread(target=listenRequest)
thread2 = threading.Thread(target=executeNotify)

# notify about completion

thread1.start()
thread2.start()
thread1.join()
thread2.join()
