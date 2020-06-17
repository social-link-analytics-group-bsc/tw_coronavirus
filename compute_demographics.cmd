#!/bin/bash
#SBATCH--job-name="covid-twitter-user-demographics"
#SBATCH--workdir=.
#SBATCH--output=demographics_%j.out
#SBATCH--error=demographics_%j.err
#SBATCH--ntasks=1
#SBATCH--time=24:00:00
#SBATCH--cpus-per-task=24

echo $(date '+%Y-%m-%d %H:%M:%S')
module purge
module load intel
module load mkl
module load python/3.7.4
source env/bin/activate
cd src
python run.py predict-user-demographics users --config_file 'config_mongo_inb.json'
echo $(date '+%Y-%m-%d %H:%M:%S')