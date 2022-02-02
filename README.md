## Requirements
- install docker 
- install docker-compose

## Docker network separation 
- add the json file as  /etc/docker/daemon.json to separate docker network and other network

## Create the AIRFLOW UID and change permission 
- run the airflow_env.sh
- change the log file permission [it already done when bash is run]

## Note change the Owner to add or remove the codes 
- sudo chown $USER:root dags/
- sudo chown $USER:root data/
- sudo chown $USER:root includes/
- sudo chown $USER:root plugins/

## UP the containers 
docker-compose up -d

## Down the containers 
docker-compose down 

# check in the localhost:8080
- Login user : airflow
- Login pass : airflow

## Bricks 
- Data cleaning process 
- Modeling 
- Descriptive stats 
- A/B testing with inferential stats

Caution : remove the airflow user and password section in docker-compose.yaml 
Note: this is not a production Docker image

