#!/bin/bash

if [ -z "$1" ]
then
    # if no date is supplied, current date is used
    DATE=`date +%Y-%m-%d`
else
    DATE=$1
fi

####
# Declare constants
####
LOGFILE=tweets_downloader.log
ERRORFILE=tweets_downloader.err
ALIAS_STORAGE="tweets_dumps"
BUCKET_NAME="covid-tweet-dumps"
SERVER_RESOURCE=${ALIAS_STORAGE}/${BUCKET_NAME}
DUMP_NAME="dump-spanish-tweets-about-covid-in-spain-v2-${DATE}.tar.gz"
BSON_PATH="dump/covid-un/spanish-tweets-about-covid-in-spain-v2-${DATE}.bson"
FULL_PATH=$SERVER_RESOURCE/$DUMP_NAME
OUTPUT_DIR="${HOME}/Downloads/dumps_tweets_esp/from_hpai"
OUTPUT_DIR_NEW_DUMP="${HOME}/Downloads/dumps_tweets_esp/to_hpai"
NEW_DUMP_NAME="dump-spanish-tweets-about-covid-in-spain-v2-${DATE}-processed.tar.gz"
NEW_DUMP_PATH=$OUTPUT_DIR_NEW_DUMP/$NEW_DUMP_NAME
COLLECTION_NAME="tweets_esp_${DATE}"
MASTER_COLLECTION="tweets_esp"
DB_NAME='tweets_covid19'
HOST='localhost'
PORT='27017'

error=0

#####
# 1. Download dump if it does not exist already
#####
if [[ ! -f ${OUTPUT_DIR}/${DUMP_NAME} ]]
then
    echo "[1/13] Downloading dump: ${DUMP_NAME}..."
    mc cp $FULL_PATH $OUTPUT_DIR 2> $ERRORFILE
    echo "Download completed, dump was saved into: ${OUTPUT_DIR}"
fi

####
# 2. Untar dump
###
if [ $? -eq 0 ]
then
    echo "[2/13] Extracting dump ${OUTPUT_DIR}/${DUMP_NAME}..."
    tar zxvf $OUTPUT_DIR/$DUMP_NAME -C $OUTPUT_DIR > $LOGFILE 2> $ERRORFILE
else
    error=1
fi


####
# 3. Restore dump
####
if [ $? -eq 0 ]
then
    echo "Extraction completed."
    echo "[3/13] Restoring dump..."
    mongorestore --host $HOST --port $PORT --db $DB_NAME --collection $COLLECTION_NAME $OUTPUT_DIR/$BSON_PATH > $LOGFILE 2> $ERRORFILE
else
    error=1
fi

####
# 4. Remove output dump directory
####
if [ $? -eq 0 ]
then
    echo "Restore completed"
    echo "[4/13] Removing dump directory..."
    rm -rf $OUTPUT_DIR/dump > $LOGFILE 2> $ERRORFILE
else
    error=1
fi

####
# 5. Creating index
####
if [ $? -eq 0 ]
then
    echo "[5/13] Creating index..."
    mongo $DB_NAME --eval "db['${COLLECTION_NAME}'].createIndex({id: 1})" > $LOGFILE 2> $ERRORFILE
else
    error=1
fi


#####
# 6. Activate running environment
#####
if [ $? -eq 0 ]
then
    echo "[6/13] Activating running environment..."
    source env/bin/activate > $LOGFILE 2> $ERRORFILE
else
    error=1
fi

####
# 7. Add date fields
####
if [ $? -eq 0 ]
then
    cd src
    echo "Running environment activated."
    echo "[7/13] Adding date fields to new tweet documents..."
    python run.py add-date-fields $COLLECTION_NAME > $LOGFILE 2> $ERRORFILE
else
    error=1
fi

####
# 8. Run sentiment analysis
####
if [ $? -eq 0 ]
then
    echo "Date fields computation completed."
    echo "[8/13] Running sentiment analysis on new tweets..."
    python run.py sentiment-analysis $COLLECTION_NAME > $LOGFILE 2> $ERRORFILE  
else
    error=1
fi


####
# 9. Generate a new dump containing sentiment analysis results
####
if [ $? -eq 0 ]
then
    echo "Sentiment analysis completed."
    echo "[9/13] Generating new dump containing results of sentiment analysis"
    mongodump --host $HOST --port $PORT --db $DB_NAME --collection $COLLECTION_NAME --out $OUTPUT_DIR_NEW_DUMP/dump > $LOGFILE 2> $ERRORFILE
else
    error=1
fi

####
# 10. Generate a new dump containing sentiment analysis results
####
if [ $? -eq 0 ]
then
    echo "Dump completed and saved into: ${OUTPUT_DIR_NEW_DUMP}."
    echo "[10/13] Compressing dump"
    tar zcvf $NEW_DUMP_PATH $OUTPUT_DIR_NEW_DUMP/dump > $LOGFILE 2> $ERRORFILE
    rm -rf $OUTPUT_DIR_NEW_DUMP/dump
else
    error=1
fi


####
# 11. Upload new dump
####
if [ $? -eq 0 ]
then
    echo "Dump completed and saved into: ${NEW_DUMP_PATH}"
    echo "[11/13] Updating dump to HPAI server..."
    mc cp $NEW_DUMP_PATH $SERVER_RESOURCE 2> $ERRORFILE
else
    error=1
fi

####
# 12. Merge recently created collection into the master collection
###
if [ $? -eq 0 ]
then    
    echo "Upload completed."
    echo "[12/13] Merging created collection ${COLLECTION_NAME} into master collection..."
    python run.py merge-collections $MASTER_COLLECTION $COLLECTION_NAME > $LOGFILE 2> $ERRORFILE
else
    error=1
fi


#####
# 13. Drop recently created collection
#####
if [ $? -eq 0 ]
then
    echo "Merge completed."
    echo "[13/13] Removing collection ${COLLECTION_NAME}..."
    python run.py drop-collection $COLLECTION_NAME
else
    error=1
fi

####
# 14. Echo a final message
####
if [ $? -eq 0 ]
then
    echo "Process has finished successfully"
    echo "For more information, check $LOGFILE"
else
    echo "There was an error running the process"
    echo "For more information, check $ERRORFILE"
fi

exit $error
