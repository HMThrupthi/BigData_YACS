import json
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns



algo = sys.argv[1]
if(algo == 'RANDOM'):
	fname = "random.txt"
elif(algo == 'RR'):
	fname = "round.txt"
elif (algo == 'LL'):
	fname = "least.txt"

else:
	exit(0)

def getData(fname):
	with open(fname) as fp:
		rx = fp.readline()
		tlog = json.loads(rx)
		rx = fp.readline()
		jlog = json.loads(rx)
		algorithm = fname.split(".")[0].upper()
		task= {'algorithm':[],'job_id': [],'task_id': [], 'time_taken': [], 'worker':[]}
		j_logs = {'job_id':[],'time_taken':[]}
		for key,value in tlog.items():
			job = key.split("_")[0]
			task['algorithm'].append(algorithm)
			task["job_id"].append(job)
			task['task_id'].append(key)
			task['time_taken'].append(value[0])
			task['worker'].append(value[1])
		
		for key,value in jlog.items():
			j_logs['job_id'].append(key)
			j_logs['time_taken'].append(value)

		return task, j_logs
		

def cal(logs,i,tab):
	print('Mean time taken for '+str(i)+' completion:', np.mean(logs['time_taken']), 'seconds\n')
	tab = tab.append({'Name' : i, 'Type' : 'mean', 'Time' : np.mean(logs['time_taken']) }, ignore_index = True)
	print('Median time taken for '+str(i)+' completion:',np.median(logs['time_taken']), 'seconds\n')
	tab = tab.append({'Name' : i, 'Type' : 'median', 'Time' : np.median(logs['time_taken']) }, ignore_index = True)
	return tab

def bar(rn):
	sns.set(style='ticks', font_scale=1)
	fig, ax = plt.subplots(figsize=(5,5))
	sns.barplot(ax=ax,data=rn,x="Type", y="Time", hue="Name", palette='deep')
	ax.set_ylabel("Time in seconds")
	ax.legend()
	ax.set_xlabel("Type of metric")
	plt.title("Time taken for task and job")
	plt.savefig(str(algo)+"MetricVsTime.png")
	plt.show()


def heat(file):
	sns.set(style='ticks', font_scale=0.5)
	fig, ax = plt.subplots(figsize=(6,6))
	group = rnn.pivot("worker","mean_time", "number_of_tasks")
	ax = sns.heatmap(group,annot=True, fmt="f")
	ax.set_xlabel("Mean Time(in s)")
	ax.set_ylabel("Worker")
	plt.title("Mean Time vs. Worker, grouped by Number of Task")
	plt.savefig(file)
	plt.show()


t_logs,j_logs = getData(fname)
algorithm = fname.split('.')[0].upper()
rr = set(t_logs['worker'])

tab = pd.DataFrame(columns = ['Name', 'Type', 'Time'])

print(rr)
df = pd.DataFrame(t_logs, index=None, columns=['algorithm','job_id','task_id','time_taken','worker'])
df = df[['worker','algorithm','task_id','time_taken']]
rnn = df.groupby(['worker']).agg(number_of_tasks=('task_id','count'),mean_time = ('time_taken','mean'))
rnn = rnn.reset_index()
print(rnn.head(10))
print("\n")	
print(f"Metrics\n")
tab = cal(t_logs,"task",tab)
tab = cal(j_logs,"job",tab)
#tab = tab.append(cat)
#print(tab)
bar(tab)
heat(str(algo)+"TimevsWorker.png")
	
