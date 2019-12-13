#!/usr/bin/env bash

if [[  -d "${APP_HOME}" ]] ; then
  source "$(dirname "$0")"/find-etl-home
fi

ALL_MODULE=(launcher sql renew)

for VAR in ${ALL_MODULE[@]} ; do

    cp ${APP_HOME}/${VAR}/target/*-1.0-SNAPSHOT.jar /home/franksen/下载
#    cp ${APP_HOME}/${VAR}/target/*-1.0-SNAPSHOT.jar /home/franksen/MyIdeaProject/ccic_etl/jars

done

if [[ $? -eq 0 ]];then
    echo "COPY SUCCESS."
else
    echo "COPY FAILED."
fi