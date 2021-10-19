#/bin/bash

#Pre-requisites: Docker, sbt(Scala Build Tool) must be installed

cd ..

pwd=$(PWD)
bind_volume_folder=access-log-analyzer-data

sbt package

rm -rf $bind_volume_folder

docker build -t access-log-analyzer:1.0.0 .

image_id=$(docker images -q access-log-analyzer)

docker run -it -v $pwd/$bind_volume_folder:/opt/tmp $image_id

#cd $bind_volume_folder/output; ls -l
