# Requirements
- install docker 
- install docker-compose

# Docker network separation 
- add the json file to  /etc/docker/daemon.json to separate docker network and other network

# Create the AIRFLOW UID and change permission 
- run the airflow_env.sh
- change the log file permission [it already done when bash is run]

# UP the containers 
docker-compose up -d

# check in the localhost:8080
- Login user : Airflow
- Login pass : Airflow
