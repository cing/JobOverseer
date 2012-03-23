"""
This script functions identically to generate_queue_report
with the exception that you supply a 3rd argument representing the name of the cluster
that you'd like to generate a report on.

Usage: python generate_cluster_report.py queue_data.db 7 SciNet
"""
from sys import argv
import sqlite3
from datetime import datetime, time, date, timedelta

if len(argv) < 4:
        raise Exception("Usage: python generate_cluster_report.py db_file.db num_days cluster_name")

filename = argv[1]
previous_days = int(argv[2])
conn = sqlite3.connect(filename)
c = conn.cursor()
c.execute("select * from jobs where cluster = '"+argv[3]+"'")
query_results = c.fetchall()

#Everything is calculated relative to the current day, but that could change in the future
nowtime = datetime.combine(date.today(), time())

cluster_use = []

#We're going to loop over the number of days in the past (-1 days, -2 days, etc.)
for prev_day in range(previous_days):
        previous_time_frame_plus_1 = timedelta(days=prev_day+1)
        previous_time_frame = timedelta(days=prev_day)

        users_per_day = {}

        for row in query_results:
            job_datetime = datetime.strptime(row[1], "%Y-%m-%d %X.%f")

            #Check to see if the job was sampled during the time window we're looping over
            if previous_time_frame < (nowtime - job_datetime) < previous_time_frame_plus_1:
                #We record the user for that job and add it to the users_per_day dictionary
                user_name = str(row[2])
                if not users_per_day.has_key(user_name):
                    users_per_day[user_name] = int(row[3])*int(row[4])
                else:
                    users_per_day[user_name] += int(row[3])*int(row[4])

        #Append the users_per_day for a given day to a big list
        cluster_use.append(users_per_day)

#Print out a nice summary of the users for X "previous_days" 
for prev_day in range(previous_days):
    previous_time_frame_plus_1 = timedelta(days=prev_day+1)
    previous_time_frame = timedelta(days=prev_day)

    print nowtime-previous_time_frame_plus_1, "to", nowtime-previous_time_frame, cluster_use[prev_day]

c.close()
