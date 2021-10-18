#/bin/bash

#Pre-requisites that the system has sbt(Scala Build Tool) installed

cd ..

pwd=$(PWD)
bind_volume_folder=access-log-analyzer-data

sbt package

rm -rf $bind_volume_folder

docker build -t access-log-analyzer:1.0.0 .

image_id=$(docker images -q access-log-analyzer)

docker run -it -v $pwd/$bind_volume_folder:/opt/tmp $image_id
