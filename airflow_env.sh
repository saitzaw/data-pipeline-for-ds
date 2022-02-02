echo -e "AIRFlOW_UID=$(id -u)\nAIRFLOW_GID=0">.env

DIR1="/logs"
DIR2="/data"

if [ -d $DIR1 ]; then 
	sudo chmod 764 $DIR1
fi

if [ -d $DIR2 ]; then
	sudo chmod 764 $DIR2
fi
