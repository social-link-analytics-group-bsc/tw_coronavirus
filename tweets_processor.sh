#!/bin/bash

if [ -z "$1" ]
then
    # if no project directory is supplied, current is used
    PROJECT_DIR=`pwd`
else
    PROJECT_DIR=$1
fi


LOGFILE=${PROJECT_DIR}/tweets_downloader.log
ERRORFILE=${PROJECT_DIR}/tweets_downloader.err
error=0


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
    source env/bin/activate
else
    error=1
fi

####
# Run sentiment analysis
####
if [ $? -eq 0 ]
then
    cd src
    echo "[1/2] Running sentiment analysis..."
    python run.py preprocess 'processed' --config_file 'config_mongo_inb.json' > $LOGFILE 2> $ERRORFILE
else
    error=1
fi

####
# Run language detection
# for spanish tweets
####
if [ $? -eq 0 ]
then
    echo "[2/2] Running language detection..."
    python run.py add-language-flag 'processed' --config_file 'config_mongo_inb.json' > $LOGFILE 2> $ERRORFILE
else
    error=1
fi

if [ $? -eq 0 ]
then
    echo "Process has finished successfully"
    echo "For more information, check $LOGFILE"
else
    echo "There was an error running the process"
    echo "For more information, check $ERRORFILE"
fi

exit $error