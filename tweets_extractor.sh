#!/bin/bash

PROJECT_DIR=`pwd`
COLLECTION_NAME='processed_new'

for arg in "$@"
do
    case $arg in
        --project_dir=*)
        PROJECT_DIR="${arg#*=}"
        shift # Remove --project_dir= from processing
        ;;        
        --collection_name=*)
        COLLECTION_NAME="${arg#*=}"
        shift # Remove --collection_name= from processing
        ;;
        --destination_dir=*)
        DESTINATION_DIR="${arg#*=}"
        shift # Remove --destination_dir= from processing
        ;;
        --lingo4g_dir=*)
        LINGO4G_DIR="${arg#*=}"
        shift # Remove --lingo4g_dir= from processing
        ;;
        --lingo4g_project_file=*)
        LINGO4G_PROJECT_FILE="${arg#*=}"
        shift # Remove --lingo4g_project_file= from processing
        ;;
    esac
done

LOG_DIR="${PROJECT_DIR}/log"

if [[ ! -d $LOG_DIR ]]
then
    `mkdir $LOG_DIR`
fi

LOGFILE=${LOG_DIR}/tweets_processor.log
ERRORFILE=${LOG_DIR}/tweets_processor.err
EVENT_LOG=${LOG_DIR}/process_events_log.csv
ENV_DIR="${PROJECT_DIR}/env"
OUTPUT_DIR="${PROJECT_DIR}/data"
OUTPUT_FILE="${OUTPUT_DIR}/tweets.json"
CONFIG_FILE_NAME='config_mongo_inb.json'
CONDA_ENV='twcovid'
NUM_TASKS=4
error=0

####
# Print a starting message
####
running_date=`date '+%Y-%m-%d'`
printf "\n\n#####\nStarting to extract tweets at ${running_date}\n######\n\n" >> $LOGFILE
start_time=`date '+%Y-%m-%d %H:%M:%S'`
echo "tweets_extractor,${running_date},${COLLECTION_NAME}'starting_extractor',${start_time}," >> $EVENT_LOG

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

echo "Exporting tweets from the collection ${COLLECTION_NAME}..."

####
# Extract tweet to JSON
####
if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    cd src
    echo "[1/${NUM_TASKS}] Exporting tweets..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "tweets_extractor,${running_date},${COLLECTION_NAME},'export_tweets',${start_time}," >> $EVENT_LOG
    python run.py export-tweets $COLLECTION_NAME $OUTPUT_FILE --config_file $CONFIG_FILE_NAME --lang 'es' >> $LOGFILE 2>> $ERRORFILE
else
    error=1
fi

if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'export_tweets',,${end_time}" >> $EVENT_LOG
    echo "[2/${NUM_TASKS}] Copying output file to the destination directory..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "tweets_extractor,${running_date},${COLLECTION_NAME},'copy_file',${start_time}," >> $EVENT_LOG
    cp $OUTPUT_FILE $DESTINATION_DIR >> $LOGFILE 2>> $ERRORFILE
else
    error=1
fi

if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'copy_file',,${end_time}" >> $EVENT_LOG
    cd $LINGO4G_DIR
    echo "[3/${NUM_TASKS}] Indexing new tweets..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`    
    echo "tweets_extractor,${running_date},${COLLECTION_NAME},'index_tweets',${start_time}," >> $EVENT_LOG
    l4g index -p ${LINGO4G_PROJECT_FILE} --force >> $LOGFILE 2>> $ERRORFILE    
else
    error=1
fi

if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${running_date},'index_tweets',,${end_time}" >> $EVENT_LOG
    echo "[4/${NUM_TASKS}] Learning embeddings..."
    start_time=`date '+%Y-%m-%d %H:%M:%S'`    
    echo "tweets_extractor,${running_date},${COLLECTION_NAME},'learn_embeddings',${start_time}," >> $EVENT_LOG
    l4g learn-embeddings -p ${LINGO4G_PROJECT_FILE} >> $LOGFILE 2>> $ERRORFILE
else
    error=1
fi

if [[ $? -eq 0 ]] && [[ $error -eq 0 ]]
then
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "tweets_extractor,${running_date},'learn_embeddings',,${end_time}" >> $EVENT_LOG    
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