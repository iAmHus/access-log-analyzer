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

ARG topN=2
ENV topNValues=$topN

COPY target/scala-2.11/access-log-analyzer_2.11-1.0.0.jar /opt/application/

COPY src/main/resources/log4j.properties /opt/application/

RUN mkdir -p /opt/tmp/input

CMD ./spark-2.4.6-bin-hadoop2.7/bin/spark-submit --verbose --files /opt/application/log4j.properties --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///opt/application/log4j.properties --deploy-mode client --master local --driver-memory 1g --class com.learning.analyzer.main.App /opt/application/access-log-analyzer_2.11-1.0.0.jar $topNValues "/opt/tmp" "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz" "https://github.com/iAmHus/datasets/blob/main/NASA_access_log_Jul95.gz?raw=true"
