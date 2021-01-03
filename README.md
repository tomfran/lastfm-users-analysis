# last.fm project
Architectures for big data's course project focused on data about songs and users coming from last.fm API.

The project runs on Google Cloud Platform.

# Architecture overview
The architecture design consists in a Linux Virtual Machine
running the code shown in the repository, to gather data about users and write it in a Google Cloud Bucket.

## Compute engine
The Linux Virtual Machine is running Ubuntu 20.04 LTS and it runs the change data capture logic daily.
### CDC logic
The Change Data Capture pattern consists in collecting only 
modified or new data and to write in the cloud bucket.

Doing so we can store only what is necessary and avoid data replication.

## Scheduling

The Virtual Machine is started daily to gather new data.

### Cloud functions
Two cloud functions manage the Virtual Machine startup and stop.

### Cloud scheduler
A scheduler launches cloud functions on a daily basis.
It firsts launches the startup function and after some time the stop function.

Note that the Virtual Machine stops itself after it completes 
the data collection phase, to avoid unnecessary run time. The
stop function is a safe net to prevent constant running of the Virtual Machine in case something goes wrong.

## Apache Spark
...