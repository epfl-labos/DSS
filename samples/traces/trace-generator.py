#!/usr/bin/python
import sys
import numpy
import random

if(len(sys.argv)!=15):
	print "Wrong nr of parameters. 15 params needed: nr jobs    job_start_times(min,max,dist)  tasks_per_job(min,max,dist)   mem_per_task(min,max,dist)  dur_per_task(min,max,dist) random_seed"
	sys.exit(1)

nr_jobs=int(sys.argv[1])

job_start_times_min=int(sys.argv[2])
job_start_times_max=int(sys.argv[3])
job_start_times_dist=sys.argv[4]

tasks_per_job_min=int(sys.argv[5])
tasks_per_job_max=int(sys.argv[6])
tasks_per_job_dist=sys.argv[7]

mem_per_task_min=int(sys.argv[8])
mem_per_task_max=int(sys.argv[9])
mem_per_task_dist=sys.argv[10]

dur_per_task_min=int(sys.argv[11])
dur_per_task_max=int(sys.argv[12])
dur_per_task_dist=sys.argv[13]

numpy.random.seed(int(sys.argv[14]))

def exponential_distribution(min_value, max_value, how_many):
	#numpy.random.exponential(scale=1.0, size=None)
	arr=numpy.random.exponential(1,how_many)	#generates numbers between 0 and 1
	arr2=[0 for i in range(how_many)]
	max_of_arr=max(arr)
	multiplier=(max_value-min_value)*1.0/max_of_arr
	for i in range(0,how_many):
		arr2[i]=int(arr[i]*multiplier+min_value)
	return arr2


def uniform_random_distribution(min_value, max_value, how_many):
	#numpy.random.uniform(low=0.0, high=1.0, size=None)
	arr=numpy.random.uniform(min_value,max_value, how_many)
	arr2=[0 for i in range(how_many)]
	for i in range(0,how_many):
		arr2[i]=int(arr[i])
	return arr2

def constant_distribution(min_value, max_value, how_many):
	#numpy.random.uniform(low=0.0, high=1.0, size=None)
	arr2=[0 for i in range(how_many)]
        rnd=random.randint(min_value, max_value)
	for i in range(0,how_many):
		arr2[i]=rnd
	return arr2




def generate_distribution(dist_type,how_many,min_value,max_value):
	if (dist_type=="exp"):
		return exponential_distribution(min_value, max_value, how_many)
	if (dist_type=="unif"):
		return uniform_random_distribution(min_value, max_value, how_many)		
	if (dist_type=="ct"):
		return constant_distribution(min_value, max_value, how_many)		


arr_job_start_times=generate_distribution(job_start_times_dist,nr_jobs,job_start_times_min,job_start_times_max)
arr_nrtasks_per_job=generate_distribution(tasks_per_job_dist, nr_jobs,tasks_per_job_min, tasks_per_job_max)
arr_dur_per_task=generate_distribution(dur_per_task_dist,nr_jobs,dur_per_task_min,dur_per_task_max)
arr_mem_per_task=generate_distribution(mem_per_task_dist,nr_jobs,mem_per_task_min,mem_per_task_max)

for job_index in range(0,nr_jobs):
	j_st_time=arr_job_start_times[job_index]*1000	
	j_nr_tasks=arr_nrtasks_per_job[job_index]
	j_tsk_dur=arr_dur_per_task[job_index]*1000
	j_tsk_mem=arr_mem_per_task[job_index]*100


	print '{\
	"am.type" : "mapreduce",\n\
	"job.start.ms" : '+str(j_st_time)+',\n\
	"job.end.ms" : 200000,\n\
	"job.queue.name" : "sls_queue_1",\n\
	"job.id" : "job_'+str(job_index)+'",\n\
	"job.user" : "default",\n\
	"job.tasks" : [\n\
{"c.host" : "/default-rack/node2354434", "c.dur" : '+str(j_tsk_dur)+', "c.prio" : 20, "c.mem" : '+str(j_tsk_mem)+', "c.type" : "reduce", "c.nr" : '+str(j_nr_tasks)+'}\n\
]\n\
}' 




