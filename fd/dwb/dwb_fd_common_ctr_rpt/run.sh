#!/bin/sh
home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt_last=`date -d "-2 days" +%Y-%m-%d`
    pt_format=`date -d "-1 days" +%Y%m%d`
    pt_format_last=`date -d "-2 days" +%Y%m%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt_last=`date -d "$1 -1 days" +%Y-%m-%d`
    pt_format=`date -d "$1" +%Y%m%d`
    pt_format_last=`date -d "$1 -1 days" +%Y%m%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt_last
echo $pt_format
echo $pt_format_last

#hive -hiveconf pt=$pt -f /mnt/vova-bigdata-scripts/fd/dwb.dwb_fd_common_ctr_rpt/dwb_fd_common_ctr.hql

#shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_common_ctr_rpt"

#hive -hiveconf pt=$pt -f /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_common_ctr_rpt/dwb_fd_common_ctr_rpt.hql


sql="
drop table  if exists tmp.tmp_fd_common_ctr;
create table tmp.tmp_fd_common_ctr as
SELECT
/*+ REPARTITION(5) */
event_name,
nvl(platform_type,'NALL') as platform_type,
nvl(country,'NALL') as country,
nvl(project,'NALL') as project,
nvl(page_code,'NALL') as page_code,
nvl(element_event_struct.list_type,'NALL') as list_name,
nvl(element_event_struct.element_name,'NALL') as  element_name,
session_id
from ods_fd_snowplow.ods_fd_snowplow_element_event
where pt='$pt'
and event_name in ('common_impression', 'common_click')
and session_id is not null
group by platform_type,country,project,page_code,element_event_struct.list_type,element_event_struct.element_name,session_id,event_name;


insert overwrite table dwb.dwb_fd_common_ctr_rpt  partition(pt='$pt')
SELECT
/*+ REPARTITION(1) */
nvl(platform_type,'all'),
nvl(country,'all'),
nvl(project,'all'),
nvl(page_code,'all'),
nvl(list_name,'all') AS list_name,
nvl(element_name,'all') AS element_name,
count(distinct if(event_name = 'common_impression', session_id, NULL)) AS impression_uv,
count(distinct if(event_name = 'common_click', session_id, NULL)) AS click_uv
from tmp.tmp_fd_common_ctr
group by platform_type,country,project,page_code,list_name,element_name with cube;


"

spark-sql \
--conf "spark.app.name=dwb_fd_common_ctr_rpt_yjzhang"   \
-d pt=$pt \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
echo "step1: dwb_fd_common_ctr_rpt table is finished !"