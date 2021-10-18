FROM ubuntu:latest
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk
RUN apt-get update
RUN apt-get install git -y
RUN apt-get update
RUN apt-get install wget -y
RUN wget "https://archive.apache.org/dist/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz"
RUN tar -xzvf spark-2.4.6-bin-hadoop2.7.tgz
RUN rm spark-2.4.6-bin-hadoop2.7.tgz

#LABEL image=test.gz-spark

COPY target/scala-2.11/access-log-analyzer_2.11-1.0.0.jar /opt/application/

RUN mkdir -p /opt/tmp/input

#RUN wget "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz" -P /opt/tmp/input/
#RUN mv /opt/tmp/input/NASA_access_log_Jul95.gz /opt/tmp/input/test.gz.gz

CMD ./spark-2.4.6-bin-hadoop2.7/bin/spark-submit --verbose --deploy-mode client --master local --driver-memory 1g --executor-memory 2g --executor-cores 1 --class com.learning.analyzer.main.App /opt/application/access-log-analyzer_2.11-1.0.0.jar 4 "/opt/tmp" "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
