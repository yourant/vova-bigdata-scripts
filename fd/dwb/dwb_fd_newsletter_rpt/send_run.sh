#!/bin/sh
home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt_last=`date -d "-2 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt_last=`date -d "$1 -1 days" +%Y-%m-%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt_last

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_newsletter_rpt"

sql="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
insert overwrite table dwb.dwb_fd_newsletter_send_rpt partition (pt)
select /*+ REPARTITION(1) */ tab1.project,
       tab1.nl_code_num,
       tab1.nl_code,
       tab1.nl_type,
       tab1.create_time,
       tab1.send_time,
       sum(tab1.total_count),
       sum(tab1.success_count),
       sum(tab1.fail_count),
       sum(tab1.open_count),
       sum(0) as unsubscribe_count,
       tab1.pt as pt
from (
      select
            case
                when lower(substr(nl_code,1,2)) = 'ad' then 'airydress'
                when lower(substr(nl_code,1,2)) = 'fd' then 'floryday'
                when lower(substr(nl_code,1,2)) = 'sd' then 'sisdress'
                when lower(substr(nl_code,1,2)) = 'td' then 'tendaisy'
            end as project,
          split(nl_code,'_')[0] as nl_code_num,
          nl_code,
          nl_type,
          TO_UTC_TIMESTAMP(create_time, 'America/Los_Angeles') as create_time,
          TO_UTC_TIMESTAMP(send_time, 'America/Los_Angeles') as send_time,
          send_count as total_count,
          arrive_count as success_count,
          (send_count-arrive_count) as fail_count,
          open_count,
          0 as unsubscribe_count,
          date(TO_UTC_TIMESTAMP(send_time, 'America/Los_Angeles')) as pt
    from ods_fd_nl.ods_fd_newsletters
    where date(TO_UTC_TIMESTAMP(send_time, 'America/Los_Angeles')) >= date_sub('$pt',15)
    and date(TO_UTC_TIMESTAMP(send_time, 'America/Los_Angeles')) <= '$pt'
    and lower(substr(nl_code,1,2)) IN ('ad','fd','sd','td')
) tab1
group by tab1.project,
         tab1.nl_code_num,
         tab1.nl_code,
         tab1.nl_type,
         tab1.create_time,
         tab1.send_time,
         tab1.pt;
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=dwb_fd_newsletter_send_gaohaitao"   --conf "spark.sql.output.coalesceNum=30" --conf "spark.dynamicAllocation.minExecutors=30" --conf "spark.dynamicAllocation.initialExecutors=50" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi

#Newsletter发送量报表
#hive -hiveconf pt=$pt -f ${shell_path}/dwb_fd_newsletter_send.hql
#if [ $? -ne 0 ];then
#  exit 1
#fi