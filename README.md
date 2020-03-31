# Exploring COVID-19 conversations on Twitter

Scripts to collect, process, and analyze publications on Twitter about COVID-19.

## Installation

1. Install requirements `pip install -r requirements.txt`
2. Rename `src/config.json.example` to `src/config.json` 
3. Set information about mongo db in `src/config.json`

## CLI

All commands must be run from the `src` directory.

### Detect language of tweets

`python run.py --detect_language [data_dir] [file_name_of_tweets]`

- *data_dir*: path to data directory and must be relative to the `src` directory
- *file_name_of_tweets*: Name of the file containing the tweets in CSV format

### Create network of users

`python run.py --create_db_users [name_of_database_of_tweets]`

- *name_of_database_of_tweets*: name of the database of tweets

### Create network of interactions

**The database of users must exist**

`python run.py --create_interaction_net`





