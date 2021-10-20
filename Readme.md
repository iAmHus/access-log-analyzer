# Access Log Analyzer

This project is a program in Scala/Spark packaged into a docker container that -

- Downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
- Determines the top-n most frequent visitors and urls for each day of the trace
- Saves the output to a directory - which binds as a docker volume to a folder (access-log-analyzer-data) on the host machine


### Running the project

###### <b>PRE-REQUISITES: Have GIT, Docker, Scala 2.11 and Scala Build Tool [https://www.scala-sbt.org/download.html] (SBT) installed</b>
- Git clone the project from here - https://github.com/iAmHus/access-log-analyzer
- Navigate to the deployment-scripts directory
- Run the script using the 'sh deploy.sh'
  
```
git clone https://github.com/iAmHus/access-log-analyzer

cd access-log-analyzer/deployment-scripts

chmod 755 deploy.sh

# Run the deployment script, with an argument specifying how many 'topN' values you would want to see
# Specifying 3, as we do here, gets the top3 most frequent URLs and the top 3 most frequent visitors


# ** PLEASE DON'T BE PUT OFF IF YOU SEE A FEW ERRORS ON THE SCREEN, THE FTP URL IS NOT TOO RELIABLE, SO THERE IS A BACKUP URL PASSED TO THE APP, SEE THE "NOTE" AT THE END OF THE README FILE ****

sh deploy.sh -n 3 

# Wait for the process to complete and for the shell to return

```

The shell script kicks off a series of commands, the last of which runs a docker file containing the spark-submit command, that takes 3 arguments :
- topN records - to be determined
- path to write the output files in the container
- URL the project downloads the data from
- Back-up URL the projects falls back to download the input

### Verifying the output

Once the steps above complete successfully and the shell prompt returns, verify the output by doing the following - 

```

cd ../access-log-analyzer-data/output

ls

# you should see two folders - topNFrequentVisitorsPerDay and topNFrequentURLsPerDay

# 'cd' into those folders, and you have the CSV files with the output data
```

- The project uses volumes - to map the output directory in the container to the host machine
- After the project runs successfully; there should be a folder in the home directory of the project i.e. one level above deployment-scripts directory called "access-log-analyzer-data"
- It should be having two folders in it  - "access-log-analyzer-data/output" containing the csv files corresponding to topNFrequentURLsPerDay AND topNFrequentVisitorsPerDay respectively

<b>Note:</b> 
- The original file location (FTP server) - ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz  has not been too reliable, working on some occasions but not on the other, so created a backup location here, hosting the same file - https://github.com/iAmHus/datasets/blob/main/NASA_access_log_Jul95.gz?raw=true