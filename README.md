## Requirements
- install docker 
- install docker-compose

## Docker network separation 
- add the json file as  /etc/docker/daemon.json to separate docker network and other network

## Create the AIRFLOW UID and change permission 
- run the airflow_env.sh

## Note change the logs file permssion
- change the log file permission [it already done when bash is run]
- sudo chmod -R 775 logs/ 

## UP the containers 
docker-compose up -d

## Down the containers 
docker-compose down 

# check in the localhost:8080
- Login user : airflow
- Login pass : airflow

## Bricks Scripts
- Data cleaning process, script and SQL 
- Modeling, script  
- Descriptive stats, RUN the jupyter notebook using voila 
- A/B testing with inferential stats, jupyter notebook using voila

### data sources 
- Postgresql 
- feather file 
- csv file 
- excle file

# Caution  
remove the airflow user and password section in docker-compose.yaml when deploy in production 

# Note
 this is not a production Docker image, it is not acceptable to use in prodution all the screte must be save in .env or exprot from the cli

