#!/bin/bash
#以天循环
#sh x.sh 20200401 20200609
stime='20201101'
etime='20201129'

while :
do
ptdate=$(date -d "${stime:0:8}" +%Y-%m-%d)
echo "$ptdate"
sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_user_coupon_rpt/run.sh  ${ptdate};
sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_user_coupon_rpt/run_rpt.sh ${ptdate};

sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_retention_activity_rpt/retention_run.sh ${ptdate};
sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_retention_activity_rpt/checkin_run.sh ${ptdate};
sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_retention_activity_rpt/play_run.sh ${ptdate};

sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_retention_activity_rpt/register_run.sh ${ptdate};
sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_retention_activity_rpt/checkin_acc_run.sh ${ptdate};
sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_retention_activity_rpt/visit_source_run.sh ${ptdate};
sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_retention_activity_rpt/run.sh ${ptdate};


stime=$(date -d "${stime:0:8} 1day" +%Y%m%d)
if [[ $stime -gt $etime ]]
then
break
fi
done