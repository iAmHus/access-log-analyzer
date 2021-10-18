# Access Log Analyzer

This project is a program in Scala/Spark that -
1. Downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
2. Determines the top-n most frequent visitors and urls for each day of the trace.
3. The application is packaged into a docker container.


### Running the project

<b>PRE-REQUISITES: Have GIT, Docker and Scala Build Tool (SBT) installed</b>
- Git clone the project from here - 
- Navigate to the deployment-scripts directory
- Run the script using the 'sh deploy.sh', which runs the spark-submit command in the docker container
- And the last line in Dockerfile is a 'spark-submit' command that takes 3 arguments :
    - topN records - to be determined
    - path to write the output files in the container
    - File location the projects downloads the input from

### Verifying the output
- The project uses volumes - to map the output directory in the container to the host machine
- After the project runs successfully; there should be a folder in the home directory of the project i.e. one level above deployment-scripts directory called "access-log-analyzer-data"
- It should be having two folders in it  - "access-log-analyzer-data/output" containing the csv files corresponding to topNFrequentURLsPerDay AND topNFrequentVisitorsPerDay respectively

####<b>Note:</b> 
- The original file location (FTP server) - ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz  has not been too reliable, working on some occasions but not on the other, so created a backup web location here, which the project uses - https://github.com/iAmHus/datasets/blob/main/NASA_access_log_Jul95.gz?raw=true