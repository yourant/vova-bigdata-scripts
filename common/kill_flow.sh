#!/bin/bash
#指定日期和引擎
exec_id=$1
sql="update azkaban.execution_flows set status=70 where exec_id=$exec_id;"
mysql -h bd-hive-metadata.cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdgroup -pnTTPdJhVpDGv5VX4z33FwtHLmIG8oS -e "${sql}"
if [ $? -ne 0 ];then
  exit 1
fi
cd /mnt/azkaban/azkaban-web
sudo bin/shutdown-web.sh
if [ $? -ne 0 ];then
  exit 1
fi
sudo bin/start-web.sh
if [ $? -ne 0 ];then
  exit 1
fi
