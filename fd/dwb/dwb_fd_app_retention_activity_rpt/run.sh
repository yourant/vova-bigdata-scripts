#!/bin/sh

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1

fi

#hive sql中使用的变量
echo $pt

shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_retention_activity_rpt"

#计算留存数据
hive -hiveconf pt=$pt -f ${shell_path}/retention.hql
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step1: retention table is finished !"

#计算访问积分页数据
hive -hiveconf pt=$pt -f ${shell_path}/checkin.hql
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step2: checkin table is finished !"

#计算访问大转盘数据
hive -hiveconf pt=$pt -f ${shell_path}/play.hql
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step3: play table is finished !"

#计算登录数据
hive -hiveconf pt=$pt -f ${shell_path}/register.hql
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step4: register table is finished !"

#计算连续签到和累计签到数据
hive -hiveconf pt=$pt -f ${shell_path}/checkin_acc.hql
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step5: checkin_acc table is finished !"

#计算访问来源数据
hive -hiveconf pt=$pt -f ${shell_path}/visit_source.hql
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step6: visit_source table is finished !"
