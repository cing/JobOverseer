"""
Here is a script that contains all the user and cluster information from
our research lab, in this script we create cluster objects
and populate them in queue information then write this to an external database
"""
from clusters import *

UserList = []
UserList.append(User("Zhuyi Xue", ["zyxue","xuezhuyi","zhuyxue12"]))
UserList.append(User("Chris Ing", ["ceing","cing"]))
UserList.append(User("Loan Huynh", ["lhuynh"]))
UserList.append(User("John Holyoake", ["holyoake"]))
UserList.append(User("Chris Neale", ["cneale","nealechr"]))
UserList.append(User("Sarah Rauscher", ["srausche"]))
UserList.append(User("David Caplan", ["dacaplan"]))
UserList.append(User("Grace Li", ["grace", "ligrace1"]))
UserList.append(User("Nilu Chakrabarti", ["nilu", "nchakrab12", "chakraba"]))
UserList.append(User("Kethika Kulleperuma", ["kethika7", "kkullepe12","kkullepe"]))
UserList.append(User("Aditi Ramesh", ["aditi"]))

ClusterList = []
ClusterStats = []

ClusterList.append(Cluster("SciNet", 8, "cing", UserList, "login.scinet.utoronto.ca"))
ClusterList[0].setQueueCommand("showq --xml") 

ClusterList.append(Cluster("Colosse", 8, "ceing", UserList, "colosse.clumeq.ca"))
ClusterList[1].setQueueCommand("qstat -xml -g d -u '*'")

ClusterList.append(Cluster("Orca", 24, "ceing", UserList, "orca.sharcnet.ca"))
ClusterList[2].setQueueCommand("/opt/sharcnet/torque/current/bin/qstat -x")

ClusterList.append(Cluster("Nestor", 8, "ceing", UserList, "nestor.westgrid.ca"))
ClusterList[3].setQueueCommand("/opt/bin/qstat -x -l nestor")

ClusterList.append(Cluster("Lattice", 8, "ceing", UserList, "lattice.westgrid.ca"))
ClusterList[4].setQueueCommand("/usr/local/torque/bin/qstat -xt")

ClusterList.append(Cluster("MP2", 24, "ingchris", UserList, "pomes-mp2.ccs.usherbrooke.ca", True))
ClusterList[5].setQueueCommand("/opt/torque/bin/qstat -x")

ClusterList.append(Cluster("Guillimin", 12, "ceing", UserList, "guillimin.clumeq.ca"))
ClusterList[6].setQueueCommand("showq --xml")

ClusterList.append(Cluster("Parallel", 12, "ceing", UserList, "lattice.westgrid.ca"))
ClusterList[7].setQueueCommand("/usr/local/torque/bin/qstat -xt")

for cluster in ClusterList[2:]:
    print "Querying ", cluster.name
    cluster.refreshQueueData()
    print "Writing ", cluster.name
    cluster.writeQueueData()

