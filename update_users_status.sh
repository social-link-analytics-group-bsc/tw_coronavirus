COLLECTION_NAME='users'
PROJECT_DIR=`pwd`
LOG_DIR="${PROJECT_DIR}/log"

if [[ ! -d $LOG_DIR ]]
then
    `mkdir $LOG_DIR`
fi

LOGFILE=${LOG_DIR}/tweets_processor.log
ERRORFILE=${LOG_DIR}/tweets_processor.err
EVENT_LOG=${LOG_DIR}/process_events_log.csv
ENV_DIR="${PROJECT_DIR}/env"
CONFIG_FILE_NAME='config_mongo_inb.json'
CONDA_ENV='twcovid'
NUM_TASKS=1
error=0

####
# Print a starting message
####
running_date=`date '+%Y-%m-%d'`
printf "\n\n#####\nStarting to update users status at ${running_date}\n######\n\n" >> $LOGFILE
start_time=`date '+%Y-%m-%d %H:%M:%S'`
echo "${running_date},'starting_processor',${start_time}," >> $EVENT_LOG

####
# Go to project directory
####
echo "Changing directory to ${PROJECT_DIR}..."
cd $PROJECT_DIR

####
# Active virtual environment
####
if [ $? -eq 0 ]
then
    echo "Activating virtual environment..."
    if [[ -d $ENV_DIR ]]
    then
        source env/bin/activate
    else
        source /opt/conda/etc/profile.d/conda.sh
        conda activate $CONDA_ENV
    fi
else
    error=1
fi

echo "Updating collection ${COLLECTION_NAME}..."

####
# Update status of users
####
if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    cd src
    echo "[1/${NUM_TASKS}] Updating collection of users..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'updating_status',${start_time}," >> $EVENT_LOG
    python run.py update-user-status users $COLLECTION_NAME --config_file $CONFIG_FILE_NAME >> $LOGFILE 2>> $ERRORFILE
else
    error=1
fi

if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'updating_status',,${end_time}" >> $EVENT_LOG
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'finished_processor',,${end_time}" >> $EVENT_LOG
else
    error=1
fi

if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    echo "Process has finished successfully"
    echo "For more information, check $LOGFILE"
else
    echo "There was an error running the process"
    echo "For more information, check $ERRORFILE"
fi

exit $error