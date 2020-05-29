#!/bin/bash

if [ -z "$1" ]
then
    # if no project directory is supplied, current is used
    PROJECT_DIR=`pwd`
else
    PROJECT_DIR=$1
fi

LOG_DIR="${PROJECT_DIR}/log"

if [[ ! -d $LOG_DIR ]]
then
    `mkdir $LOG_DIR`
fi

LOGFILE=${LOG_DIR}/tweets_processor.log
ERRORFILE=${LOG_DIR}/tweets_processor.err
EVENT_LOG=${LOG_DIR}/process_events_log.csv
ENV_DIR="${PROJECT_DIR}/env"
COLLECTION_NAME='processed'
CONFIG_FILE_NAME='config_mongo_inb.json'
CONDA_ENV='twcovid'
NUM_TASKS=7
error=0

####
# Print a starting message
####
running_date=`date '+%Y-%m-%d'`
printf "\n\n#####\nStarting to process tweets at ${running_date}\n######\n\n" >> $LOGFILE
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

####
# Run sentiment analysis
####
if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    cd src
    echo "[1/${NUM_TASKS}] Running sentiment analysis..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'analyzing_sentiments',${start_time}," >> $EVENT_LOG
    python run.py preprocess $COLLECTION_NAME --config_file $CONFIG_FILE_NAME >> $LOGFILE 2>> $ERRORFILE    
else
    error=1
fi

####
# Run language detection
# for spanish tweets
####
if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'analyzing_sentiments',,${end_time}" >> $EVENT_LOG
    echo "[2/${NUM_TASKS}] Running language detection..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'detecting_languages',${start_time}," >> $EVENT_LOG
    yesterday_date=`date --date=yesterday +%Y-%m-%d`
    python run.py add-language-flag $COLLECTION_NAME --config_file $CONFIG_FILE_NAME --tweets_date $yesterday_date >> $LOGFILE 2>> $ERRORFILE    
else
    error=1
fi

####
# Add Spain location flags
####
if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'detecting_languages',,${end_time}" >> $EVENT_LOG
    echo "[3/${NUM_TASKS}] Adding Spain location flags..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'adding_locations',${start_time}," >> $EVENT_LOG
    python run.py add-location-flags $COLLECTION_NAME --config_file $CONFIG_FILE_NAME >> $LOGFILE 2>> $ERRORFILE
else
    error=1
fi


####
# Add query version flag
####
if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'adding_locations',,${end_time}" >> $EVENT_LOG
    echo "[4/${NUM_TASKS}] Adding query version flag..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'adding_query_versions',${start_time}," >> $EVENT_LOG
    python run.py add-query-version-flag $COLLECTION_NAME --config_file $CONFIG_FILE_NAME >> $LOGFILE 2>> $ERRORFILE
else
    error=1
fi

####
# Add complete text flag
####
if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'adding_query_versions',,${end_time}" >> $EVENT_LOG
    echo "[5/${NUM_TASKS}] Adding complete text flag..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'adding_complete_text',${start_time}," >> $EVENT_LOG
    python run.py add-complete-text-flag $COLLECTION_NAME --config_file $CONFIG_FILE_NAME >> $LOGFILE 2>> $ERRORFILE
else
    error=1
fi

####
# Update metrics
####
if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'adding_complete_text',,${end_time}" >> $EVENT_LOG
    echo "[6/${NUM_TASKS}] Updating metrics..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'updating_metrics',${start_time}," >> $EVENT_LOG
    python run.py update-tweet-metrics $COLLECTION_NAME --config_file $CONFIG_FILE_NAME >> $LOGFILE 2>> $ERRORFILE
else
    error=1
fi

####
# Add tweet type flag
####
if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'updating_metrics',,${end_time}" >> $EVENT_LOG
    echo "[7/${NUM_TASKS}] Adding tweet type flag..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'add_type_flag',${start_time}," >> $EVENT_LOG
    python run.py add-tweet-type-flag $COLLECTION_NAME --config_file $CONFIG_FILE_NAME >> $LOGFILE 2>> $ERRORFILE
else
    error=1
fi

if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'add_type_flag',,${end_time}" >> $EVENT_LOG
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