# Exploring COVID-19 conversations on Twitter

Scripts to collect, process, and analyze publications on Twitter about COVID-19.

## Installation

1. Install requirements `pip install -r requirements.txt`
2. Rename `src/config.json.example` to `src/config.json` 
3. Set information about mongo db in `src/config.json`

## Command Line Interface

All commands must be run from the `src` directory.

### Detect language of tweets

`python run.py detect-language [data_dir] [file_name_of_tweets]`

- *data_dir*: path to data directory and must be relative to the `src` directory
- *file_name_of_tweets*: Name of the file containing the tweets in CSV format

### Create network of users

`python run.py create-db-users [collection_name]`

- *collection_name*: name of collection that contains tweets

### Create network of interactions

**The database of users must exist**

`python run.py --create_interaction_net`

The resulting network is stored in `sna/gefx` under the name `network_`[today_date].gefx





