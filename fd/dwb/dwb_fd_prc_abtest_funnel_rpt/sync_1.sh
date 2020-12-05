#!/bin/bash
#以天循环
#sh x.sh 20200401 20200609
stime='20201015'
etime='20201031'

while :
do
ptdate=$(date -d "${stime:0:8}" +%Y-%m-%d)
echo "$ptdate"
sh run.sh ${ptdate}
stime=$(date -d "${stime:0:8} 1day" +%Y%m%d)
if [[ $stime -gt $etime ]]
then
break
fi
done
