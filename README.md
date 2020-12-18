# Infrastructure to collect, process, analyze tweets about COVID-19

The repository contains scripts and software modules used to collect, process,
and analyze tweets about COVID-19. A MongoDB database is employed to store the
downloaded and processed data. The script `tweets_processor.sh` is the 
responsible for the heavy-load of processing tasks. It computes the type,
extracts the complete text, identify the location in Spain, calculate the sentiment, 
infers the language, and compute metrics (retweets, favorites) of tweets. The script
also updates the collection of users and it is programmed to be executed daily
to process the tweets that get into the database lastly.

The directory `data` contains some datasets that help in processing tweets. For
example the dataset `banned_accounts.csv` includes a list of twitter account that
we decided to exclude from our collection of tweets. The datasets `demonyms_spain.csv` and
`places_spain*.*` are used in the task of identifying the location of tweets. The 
dataset `keywords_covid.txt` holds the list of keywords we used to find tweets. The
datasets `query_version_history.txt` and `query_versions.csv` shows the different 
queries we employed in our searches on Twitter. The queries evolved over time and 
in these files all different versions are reported together with usage start and
end date.

The directory `legacy` includes scripts and software code that were used in the
past and they are made available in case they can be useful in the future.

The core of the infrastructure is in `src`, which is organized in three main modules,
`data_loader.py`, `data_explorter.py`, and `data_wrangler.py` to load, export, and
process data, respectively. Functions implemented in these modules are expected to be
called from the script `run.py`, which is the main script to run processing and analysis.
Complementary the directory `utils` contains utilitarian classes that are key in
the execution of the loading, exporting, and processing tasks. Next, the complete
structure of the directory is presented.

```
...
├── src                                 <- Source code of the infrastructure
│   ├── __init__.py                     <- Makes src a Python module
│   ├── run.py                          <- Main script to run processing and analysis
|   |── test.py                         <- Script with testcases to evalute code
|   |── report_generator.py             <- Analysis function used in reporting
|   |── network_analysis.py             <- Class used to conduct network analysis
|   |── config.json.example             <- Example of a configuration file
│   ├── utils
│   │   └── __init__.py                 <- Makes utils a Python module
│   │   └── db_manager.py               <- Class to operate the MongoDB database
│   │   └── demographic_detector.py     <- Class to infer demographich features of users
│   │   └── embedding_trainer.py        <- Class to train word-embeddings from a corpus of tweets
│   │   └── figure_maker.py             <- Class to plot figures
│   │   └── language_detector.py        <- Class to detect language of tweets
│   │   └── location_detector.py        <- Class to detect location of tweets or users
│   │   └── sentiment_analyzer.py       <- Class to compute polarity score of tweets
│   │   └── utils.py                    <- General utilitarian
│   │   └── lib                         
│   │       └── dependency.txt          <- Instructions to download dependency of fasttext
│   │       └── lid.176.bin             <- Dependency binary of fasttext
│   │   └── dashboard                         
│   │       └── app.py                  <- Proof-of-concept of an interactive dashboard
│   │       └── assets                  <- Directory with the assets required by the dashboard 
...
```

## Demographic detector (M3Inference)

The demographic characteristics of users are calculated using the library 
[M3Inference](https://github.com/euagendas/m3inference), a deep-learning system 
for demographic inference. Details on how M3Inference works under the hood can
be learn in the article [Demographic Inference and Representative Population Estimates from Multilingual Social Media Data](https://dl.acm.org/doi/10.1145/3308558.3313684).

M3Inference helps us to derive the gender and age of the users as well as to 
identify which Twitter users are controlled by organizations and which represent
people. A manual inspection on a sample of the M3Inference results showed a low
accuracy of age inference, hence, it was discarded.

## Language detector

...

## Location detector

...

## Sentiment analyzer

...

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





