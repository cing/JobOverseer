"""
This is a script that takes a datafile and number of days
as an argument. 

It returns the average number of cores used for a given week
for each cluster in the database
"""

import sqlite3
import datetime
from sys import argv

#This is the path to some queue_data SQL
filename = argv[1]

#This is the number of X days to average
previous_days = int(argv[2])

#This is an important variable to know about.
#Since we don't know apriori how many times we've collected qstat data
#in a given 24 hour period, we have to assume that different qstat outputs
#are separated by at least X number of minutes.
run_threshold = datetime.timedelta(minutes=5)

conn = sqlite3.connect(filename)
c = conn.cursor()

#we'd like to know about the last week of jobs on all clusters
#sure we could make a datetime selection but why not just work with things in python
c.execute('select * from jobs')
#TODO: Make it only select based on the datetime of the value in "previous_days"

query_results = c.fetchall()

#Everything is calculated relative to the current day, but that could change in the future
nowtime = datetime.datetime.now()

#This is a list for each day of the week of the past X days
#inside it will be a dictionary of ClusterName and "average cores used" pairs
#once everything is properly normalized
cores_per_day = []

#We're going to loop over the number of days in the past (-1 days, -2 days, etc.)
for prev_day in range(previous_days):
    previous_time_frame_plus_1 = datetime.timedelta(days=prev_day+1)
    previous_time_frame = datetime.timedelta(days=prev_day)
    
    #You can reassure yourself by printing out the timeframes we're checking
    #print previous_time_frame_plus_1, previous_time_frame

    #this is a temporary dictionary for each day that holds the total cores used
    cluster_cores={}

    #this stores how many times a cluster has been read to normalize the previous value
    cluster_datapoints={}

    #This stores a datetime of the last time a cluster had a job read 
    cluster_lastread={}

    #We'll be parsing the list of all jobs each time, kind of wasteful when the DB is huge
    #but what the heck... 
    for row in query_results:
        job_datetime = datetime.datetime.strptime(row[1], "%Y-%m-%d %X.%f")

        #Check to see if the job was sampled during the time window we're looping over
        if previous_time_frame < (nowtime - job_datetime) < previous_time_frame_plus_1:
            cluster_name = str(row[0])
            if not cluster_cores.has_key(cluster_name):
                cluster_cores[cluster_name] = 0.
                cluster_datapoints[cluster_name] = 1.
                cluster_lastread[cluster_name] = job_datetime

            #Make sure to only parse the running jobs, not the queued ones!
            if row[5] == "R":
                cluster_cores[cluster_name] += int(row[3])*int(row[4])

                #Only if we have a job sampled more than run_threshold minutes from now 
                #do we have a new datapoint
                if (job_datetime - cluster_lastread[cluster_name]) > run_threshold:
                    cluster_datapoints[cluster_name] += 1
                    cluster_lastread[cluster_name] = job_datetime

    #Normalize cores by the number of data collection events
    for cluster_name, cores in cluster_cores.iteritems():
        cluster_cores[cluster_name] /= cluster_datapoints[cluster_name]

    cores_per_day.append(cluster_cores)
        
print cores_per_day
c.close()
