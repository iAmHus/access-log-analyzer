#/bin/bash

#Pre-requisites: Docker, sbt(Scala Build Tool) must be installed

cd ..

while getopts n: flag

do
    case "${flag}" in
        n) topN=${OPTARG};;
    esac
done

pwd=$(PWD)
bind_volume_folder=access-log-analyzer-data

sbt package

rm -rf $bind_volume_folder

docker build -t access-log-analyzer:1.0.0 --build-arg topN=$topN .

image_id=$(docker images -q access-log-analyzer)

docker run -it -v $pwd/$bind_volume_folder:/opt/tmp $image_id

#cd $bind_volume_folder/output; ls -l
