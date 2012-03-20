from connection_handler import Connection
import operator
import xml.etree.ElementTree as xml
import sqlite3
import os
import datetime

class Cluster():
    """
    Cluster
    
    This is a compute resource that can be accessed via SSH. 

    It will presumably be accessed in order to remotely execute
    commands, parse the output, and save the data in a local database.
    """

    def __init__(self, name, cores, user, userlist, host, virtual_processors_flag=False):
        self.name = name
        self.cores = cores
        self.user = user
        self.userlist = userlist
        self.host = host
        self.queue_data = None
        self.vp_flag = virtual_processors_flag

    def executeCommand(self, command):
        """
        This creates an SSH connection using the stored login details
        and returns a string of the output from the command
        """
        newconn = Connection(self.host, self.user)
        #The trouble is that you get a weird list instead of a string
        #so just merge those from the get-go
        result = "".join(newconn.execute(command))
        newconn.close()
        return result

    def isUser(self, username):
        """
        The function performs two tasks!
        1) It returns a string if isUser is in self.userlist
           of the Cluster and otherwise None.
        2) If a username is found in self.userlist,
           it always returns the first username.

        This is useful because we want to be able to look
        through our saved queue database later and select
        all the jobs for a particular user.
        """
        for user in self.userlist:
            for alias in user.login_names:
                if username == alias:
                    #ensure we always return the first alias
                    return user.login_names[0]
        return None

        #This would be the pro-python version of the code
        #in order to return true or false if a user exists
        #all_logins = [x.login_names for x in self.userlist]
        #merged_logins = reduce(operator.add, all_logins)
        #return any([login == username for login in merged_logins])


    def setQueueCommand(self, joblist_command):
        """
        This is a functiont to set the joblist (qstat)
        command on a particular cluster
        """
        self.joblist_command = joblist_command

    def refreshQueueData(self):
        """
        This function connects to the cluster and runs a previously set 
        queue command, parses the data and stores it in self.queue_data

        The datatype of self.queue_data is a list of dictionaries.
        Each dictionary has the keys "Job_Owner", "Cores", "Nodes", and
        "State". Cores represents the cores per node, and nodes is the 
        number of nodes used for an HPC computation. State is either "R"
        for a running job, or "W" for all other states. Becareful if
        qstat returns completed jobs because they will likely be
        considered "W" for waiting. 
        """
        if self.joblist_command is not None:
            result = self.executeCommand(self.joblist_command)
        else:
            print "ERROR: Joblist command not set!"
            return False
        jobs = []

        #Assuming that the command is showq --xml, parse the output
        if self.joblist_command.startswith("showq --xml"):
            raw_data = xml.fromstring(result)
            
            #This gets a list of ElementTree.Element objects
            #for each of the three queues
            queues_xml = raw_data.findall("queue")

            #We're going to create a new list that contains
            #only jobs from the three queues but from our group
            for queue in queues_xml:
                temp_list = []
                for job in queue.findall("job"):
                    temp_job = {}
                    valid_user = self.isUser(job.get("User"))
                    if valid_user is not None:
                        temp_job["Job_Owner"]=valid_user

                        #showq XML output may not have reqnodes
                        #if the user didn't specify it.
                        #Simplest solution is to hardcode the cluster cores.
                        #Pray for mojo that your cluster nodes are uniform...
                        if job.get("ReqNodes") is not None:
                            temp_job["Cores"]=self.cores
                            temp_job["Nodes"]=int(job.get("ReqNodes"))
                        else:
                            temp_job["Cores"]=int(job.get("ReqProcs"))
                            temp_job["Nodes"]=1

                        if job.get("State") == "Running":
                            temp_job["State"]="R"
                        else:
                            temp_job["State"]="W"

                        jobs.append(temp_job)

        #This is the qstat version from Torque (2.5.2)
        #Torque must be in the qstat path for this to parse!
        elif (self.joblist_command.find("torque") != -1):
            #For some reason the result of this command sometimes isn't XML
            #because of ampersands in certain places in the output
            #See: http://www.w3.org/TR/2006/REC-xml-20060816/#syntax
            raw_data = xml.fromstring(result.translate(None, "&"))
            for job in raw_data.findall("Job"):
                #The following line would extract the entire dictionary
                #of the XML as tag/text entries but it doesn't work for
                #subelements. This is fixed in Python2.7 with DFS iter
                #temp_job=dict([(param.tag, param.text) for param in job])
                
                valid_user = self.isUser(job.find("Job_Owner").text.split("@")[0])
                if (valid_user is not None) and (job.find("job_state").text != "C"):
                    temp_job={}
                    temp_job["Job_Owner"]=valid_user
                    temp_job["Nodes"]=job.find("Resource_List").find("nodes").text.split(":ppn=")[0]

                    #ppn in qstat output may not represent the physical cores, but rather a 
                    #designated compute slot. This flag will override the cores output
                    #and use the server.cores value instead.
                    if self.vp_flag:
                        temp_job["Cores"]=self.cores
                    else:
                        temp_job["Cores"]=job.find("Resource_List").find("nodes").text.split(":ppn=")[1]

                    if job.find("job_state").text == "R":
                        temp_job["State"]="R"
                    else:
                        temp_job["State"]="W"
                    
                    jobs.append(temp_job)


        #This is also qstat from Torque but version 2.4.6
        #Remember, you can check version with qstat --version
        elif (self.joblist_command.find("/opt/bin/qstat") != -1):
            raw_data = xml.fromstring(result.translate(None, "&"))
            for job in raw_data.findall("Job"):
                valid_user = self.isUser(job.find("Job_Owner").text.split("@")[0])
                if valid_user is not None:
                    temp_job={}
                    temp_job["Job_Owner"]=valid_user
                    if job.find("Resource_List").find("nodes") is not None:
                        temp_job["Nodes"]=int(job.find("Resource_List").find("nodes").text.split(":ppn=")[0])

                        #If the number of cores returned by ppn is just a number of compute slots
                        #then override the output and use self.cores
                        if self.vp_flag:
                            temp_job["Cores"]=self.cores
                        else:
                            temp_job["Cores"]=int(job.find("Resource_List").find("nodes").text.split(":ppn=")[1])
                    else:
                        temp_job["Nodes"]=1

                        #Same thing here!
                        if self.vp_flag:
                            temp_job["Cores"]=self.cores
                        else:
                            temp_job["Cores"]=int(job.find("Resource_List").find("procs").text)

                    if job.find("job_state").text == "R":
                        temp_job["State"]="R"
                    else:
                        temp_job["State"]="W"

                    jobs.append(temp_job)

        #This is the qstat version from Grid Engine!
        elif self.joblist_command.startswith("qstat"):
            raw_data = xml.fromstring(result)

            #there are only 2 queues returned, running and queued
            for queue in raw_data:
                temp_list = []
                for job in queue.findall("job_list"):
                    #here's a pro hacker one liner where i make a dictionary
                    #out of all the tag and texts inside each job element
                    #temp_job=dict([(param.tag, param.text) for param in job])

                    #this filters out all job owners who aren't in the lab
                    valid_user = self.isUser(job.find("JB_owner").text)
                    if valid_user is not None:
                        temp_job={}
                        temp_job["Job_Owner"]=valid_user

                        #this qstat output doesn't really display nodes/cores
                        #like the others... enragingly, so we use self.cores
                        temp_job["Cores"]=self.cores
                        temp_job["Nodes"]=int(job.find("slots").text)/self.cores

                        if job.find("state").text == "r":
                            temp_job["State"]="R"
                        else:
                            temp_job["State"]="W"

                        jobs.append(temp_job)

        else:
            print "ERROR: I don't know how to parse that joblist command yet!"
        
        self.queue_data = jobs
        return True

    def writeQueueData(self, filename="queue_data.db"):
        """
        If refreshQueueData has been run, it writes this data
        to an external database passed in filename in SQL lite
        """ 
        #Check to see if the database exists
        if not os.path.exists(filename):
            conn = sqlite3.connect(filename)
            c=conn.cursor()
            c.execute('''create table jobs
                (cluster text, stamp datetime, user text, cores real, nodes real, status text)''')
        else:
            conn = sqlite3.connect(filename)
            c=conn.cursor()

        if self.queue_data is not None:
            #The following pro-python hacker line would
            #Convert my list of dictionarys to a list of tuples of dict values
            #but its too complicated to add a timestamp and a cluster name in there:
            #writejobs = [tuple(jobdict.values()) for jobdict in self.queue_data]

            writerows = []
            #loop over each job dictionary in the queue data
            for jobdict in self.queue_data:
                #create a list that represents a row then cast it to tuple
                row = [self.name]
                row.extend([datetime.datetime.now()])
                row.extend(jobdict.values())
                writerows.append(tuple(row))

            #write the list of tuple rows to the database
            for t in writerows:
                c.execute('insert into jobs values (?,?,?,?,?,?)', t)
        
        conn.commit()
        c.close()


class User():
    """
    This represents a cluster user, there's nothing really fancy about it
    It's basically just a datatype in order to store a list of login names.

    FYI: The reason you'd want to have more than one login_name is because
         you could have different usernames on different clusters and when 
         we parse the output of a command like qstat across all machines, we 
         want to be able to say: Cluster #1 has a job by cing and Cluster #2 
         has a job by chrising25, hold on a second... thats the same dude!
    """

    def __init__(self, name, login_names):
        self.name = name
        self.login_names = login_names


if __name__ == '__main__':
    # A Userlist is required for building clusters
    # it represents the users of that cluster, and it could just be a list of 1 User (you!)
    UserList = []
    UserList.append(User("Chris Ing", ["ceing","cing"]))

    # It makes the most sense to use this application when you want to run qstat
    # on many machines, so why not make a list of them?
    ClusterList = []
    ClusterList.append(Cluster("Cluster Name", 24, "user_name_to_login_with", UserList, "yourcluster.ca"))

    # Unfortunately, no automation was included in order to detect the appropriate qstat command
    # for each cluster. You may have to figure out this with trial and error. XML output is key.
    ClusterList[0].setQueueCommand("qstat -x")

    # For each cluster in your cluster list, run the queue command which we added above
    # and optionally write that information to a SQLite database
    for cluster in ClusterList:
        print cluster.name
        cluster.refreshQueueData()
        print cluster.queue_data
        #cluster.writeQueueData()

