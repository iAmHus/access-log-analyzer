# Access Log Analyzer

This project is a program in Scala/Spark that -
1. Downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
2. Determines the top-n most frequent visitors and urls for each day of the trace.
3. The application is packaged into a docker container.