#!/bin/bash
current=`date -d "-7 day" +%Y-%m-%d`
timeStamp=`date -d "$current" +%s`
#将current转换为时间戳，精确到毫秒
currentTimeStamp=$((timeStamp*1000+`date "+%N"`/1000000))
echo $currentTimeStamp

sql="delete from azkaban.execution_flows where start_time<$currentTimeStamp;"
mysql -h bd-hive-metadata.cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdgroup -pnTTPdJhVpDGv5VX4z33FwtHLmIG8oS -e "${sql}"
echo "$sql"
if [ $? -ne 0 ]; then
  exit 1
fi
sql="delete from azkaban.execution_jobs where start_time<$currentTimeStamp;"
echo "$sql"
mysql -h bd-hive-metadata.cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdgroup -pnTTPdJhVpDGv5VX4z33FwtHLmIG8oS -e "${sql}"
if [ $? -ne 0 ]; then
  exit 1
fi

