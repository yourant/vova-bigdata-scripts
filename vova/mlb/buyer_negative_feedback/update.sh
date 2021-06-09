#!/bin/bash

echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

echo "----------删除用户负反馈过期数据-------"
# 逻辑sql
sql="
delete from themis.mlb_vova_buyer_negative_feedback
where create_time <= DATE_SUB(NOW(), INTERVAL 90 day)
;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
