#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
###逻辑sql
#依赖的表，ods_vova_vts.ods_vova_product_upload_excel,ods_vova_vts.ods_vova_merchant_login_log
#增量历史数据
sql="
ALTER TABLE dwd.dwd_vova_fact_mbrmct_mct_cd DROP IF EXISTS PARTITION (pt='$pre_date');
--md5
insert into table dwd.dwd_vova_fact_mbrmct_mct_cd partition(pt='$pre_date')
select t1.datasource,t1.mct_id,t1.id,t1.start_dt,t1.end_dt,0,t2.cnt_td,-1,t1.act_type from
(
select distinct
'vova' datasource,
merchant_id mct_id,
checksum as id,
first_value(create_time) over (partition by merchant_id,checksum order by create_time) as start_dt,
first_value(last_update_time) over (partition by merchant_id,checksum order by last_update_time desc) as end_dt,
2 act_type
from ods_vova_vts.ods_vova_product_upload_excel where upload_type = 0 AND state = 'FINISHED' AND checksum != ''
) t1
left outer join
(
select
'vova' datasource,
merchant_id mct_id,
checksum as id,count(1) cnt_td
from ods_vova_vts.ods_vova_product_upload_excel where upload_type = 0 AND state = 'FINISHED' AND checksum != ''
group by merchant_id,checksum
) t2
on t1.mct_id=t2.mct_id and t1.id=t2.id ;
--商户登录ip
insert into table dwd.dwd_vova_fact_mbrmct_mct_cd partition(pt='$pre_date')
select t1.datasource,t1.mct_id,t1.id,t1.start_dt,t1.end_dt,0,t2.cnt_td,-1,t1.act_type from
(
select distinct
'vova' datasource,
merchant_id mct_id,
ip as id,
first_value(login_time) over (partition by merchant_id,ip order by login_time) as start_dt,
first_value(login_time) over (partition by merchant_id,ip order by login_time desc) as end_dt,
1 act_type
from ods_vova_vts.ods_vova_merchant_login_log
) t1
left outer join
(
select
'vova' datasource,
merchant_id mct_id,
ip as id,count(1) cnt_td
from ods_vova_vts.ods_vova_merchant_login_log group by merchant_id,ip
) t2
on t1.mct_id=t2.mct_id and t1.id=t2.id ;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=dwd_vova_fact_mbrmct_mct_cd" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
