#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

date_pre1=`date -d $cur_date"-1 day" +%Y-%m-%d`
date_pre3=`date -d $cur_date"-3 day" +%Y-%m-%d`
date_pre7=`date -d $cur_date"-7 day" +%Y-%m-%d`
date_pre15=`date -d $cur_date"-15 day" +%Y-%m-%d`
date_pre30=`date -d $cur_date"-30 day" +%Y-%m-%d`

sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_user_cohort/update.sh ${cur_date}
echo "${cur_date}"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_user_cohort/update.sh ${date_pre1}
echo "${date_pre1}"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_user_cohort/update.sh ${date_pre3}
echo "${date_pre3}"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_user_cohort/update.sh ${date_pre7}
echo "${date_pre7}"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_user_cohort/update.sh ${date_pre15}
echo "${date_pre15}"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_user_cohort/update.sh ${date_pre30}
echo "${date_pre30}"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
