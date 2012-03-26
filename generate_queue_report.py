"""
This is a script that takes a datafile and number of days
as an argument. 

It returns the average number of cores used for a given week
for each cluster in the database
"""

import sqlite3
from datetime import datetime, time, date, timedelta
from sys import argv

#This is the path to some queue_data SQL
filename = argv[1]

#This is the number of X days to average
previous_days = int(argv[2])

#This is an important variable to know about.
#Since we don't know apriori how many times we've collected qstat data
#in a given 24 hour period, we have to assume that different qstat outputs
#are separated by at least X number of minutes.
run_threshold = timedelta(minutes=5)

conn = sqlite3.connect(filename)
c = conn.cursor()

#we'd like to know about the last week of jobs on all clusters
#sure we could make a datetime selection but why not just work with things in python
c.execute('select * from jobs')
#TODO: Make it only select based on the datetime of the value in "previous_days"

query_results = c.fetchall()

c.close()

#Everything is calculated relative to the current day, but that could change in the future
nowtime = datetime.combine(date.today(), time())

#This is a list for each day of the week of the past X days
#inside it will be a dictionary of ClusterName and "average cores used" pairs
#once everything is properly normalized
cores_per_day = []

#We're going to loop over the number of days in the past (-1 days, -2 days, etc.)
for prev_day in range(previous_days):
    previous_time_frame_plus_1 = timedelta(days=prev_day+1)
    previous_time_frame = timedelta(days=prev_day)
    
    #You can reassure yourself by printing out the timeframes we're checking
    #print nowtime-previous_time_frame_plus_1, nowtime-previous_time_frame

    #this is a temporary dictionary for each day that holds the total cores used
    cluster_cores={}

    #this stores how many times a cluster has been read to normalize the previous value
    cluster_datapoints={}

    #This stores a datetime of the last time a cluster had a job read 
    cluster_lastread={}

    #We'll be parsing the list of all jobs each time, kind of wasteful when the DB is huge
    #but what the heck... 
    for row in query_results:
        job_datetime = datetime.strptime(row[1], "%Y-%m-%d %X.%f")

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

print "--------------"
print "Daily Usage:"
print "--------------"

#output the average cores used on each cluster for every day for the given time period
for prev_day in range(previous_days):
    previous_time_frame_plus_1 = timedelta(days=prev_day+1)
    previous_time_frame = timedelta(days=prev_day)
    
    print nowtime-previous_time_frame_plus_1, "to", nowtime-previous_time_frame, cores_per_day[prev_day]


#now we'd like to display the average usage across the time period
#first we need to fix the days that do not have any usage and are not keys
if previous_days > 1:
    #loop over all pairs of days
    for day1 in cores_per_day:
        for day2 in cores_per_day[1:]:
            #if any of the cluster names doesn't match
            if set(day1.keys()) != set(day2.keys()):
                for cluster in day1.keys():
                    if not cluster in day2:
                        day2[cluster]=0.
                for cluster in day2.keys():
                    if not cluster in day1:
                        day1[cluster]=0.

            if not set(day1.keys()) == set(day2.keys()): raise AssertionError

print "--------------"
print "Average Usage:"
print "--------------"

#jedi python line to sum up across all days
avg_cores_per_day = reduce(lambda x, y: dict((k, v + y[k]) for k, v in x.iteritems()), cores_per_day)
#divide by number of days
for cluster_name, cores in avg_cores_per_day.iteritems():
    avg_cores_per_day[cluster_name] /= previous_days
    print(cluster_name + " %.2f" % round(avg_cores_per_day[cluster_name],2))
