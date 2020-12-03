#!/bin/sh
home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt_last=`date -d "-2 days" +%Y-%m-%d`
    dt_format=`date -d "-1 days" +%Y%m%d`
    dt_format_last=`date -d "-2 days" +%Y%m%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    dt=$1
    dt_last=`date -d "$1 -1 days" +%Y-%m-%d`
    dt_format=`date -d "$1" +%Y%m%d`
    dt_format_last=`date -d "$1 -1 days" +%Y%m%d`

fi

#hive sql中使用的变量
echo $dt
echo $dt_last
echo $dt_format
echo $dt_format_last

#shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_page_data_rpt"

#计算留存数据
#hive -hiveconf dt=$dt -f ${shell_path}/dwb_fd_page_data_rpt.hql




sql="
drop table tmp.tmp_fd_page_data_rpt;
create table tmp.tmp_fd_page_data_rpt as
SELECT
/*+ REPARTITION(10) */
nvl(project,'NALL'),
nvl(country,'NALL'),
nvl(platform_type,'NALL'),
nvl(os_name,'NALL'),
nvl(app_version,'NALL'),
case
       when platform = 'web' and session_idx = 1 then 'new'
       when platform = 'web' and session_idx > 1 then 'old'
       when platform = 'mob' and session_idx = 1 then 'new'
       when platform = 'mob' and session_idx > 1 then 'old'
end  as is_new_user,
nvl(page_code,'NALL'),
session_id
from ods_fd_snowplow.ods_fd_snowplow_view_event
where pt='$pt'
and session_id is not null;


insert overwrite table  dwb.dwb_fd_page_data_rpt partition (pt='$pt')

select  /*+ REPARTITION(1) */
       project,
       country,
       platform_type,
       os_name,
       app_version,
       is_new_user,
       page_code,
       count(session_id),
       count(distinct session_id)
from tmp.tmp_fd_page_data_rpt;




"

spark-sql \
--conf "spark.app.name=dwb_fd_page_data_rpt_yjzhang"   \
-d pt=$pt \
-e "$sql"


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "page data report  table is finished !"
