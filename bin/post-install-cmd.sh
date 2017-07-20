#!/bin/bash

app_base=`dirname $(readlink -f $0)`
app_base=`dirname ${app_base}`
app_base=`dirname ${app_base}`
app_base=`dirname ${app_base}`

cd ${app_base}/app
echo "Updating the Database for the tables needed by this plugin."
Console/cake schema update -y -p Snapshot
cd ${app_base}