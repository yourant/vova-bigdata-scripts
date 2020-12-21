#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新用户首单
sql="
insert overwrite table tmp.tmp_vova_buyer_first_pay
select 'vova' as datasource,
    oi.user_id as buyer_id,
    min(order_id) as first_order_id,
    min(order_time) as first_order_time,
    min(pay_time) as first_pay_time
from ods_vova_vts.ods_vova_order_info oi
where oi.pay_status >= 1
group by user_id;

insert overwrite table tmp.tmp_vova_buyer_first_refund
select 'vova' as datasource,
    oi.user_id          AS buyer_id,
    min(rr.create_time) AS first_refund_time
FROM ods_vova_vts.ods_vova_refund_reason rr
         INNER JOIN ods_vova_vts.ods_vova_order_goods og ON og.rec_id = rr.order_goods_id
         INNER JOIN ods_vova_vts.ods_vova_order_info oi ON oi.order_id = og.order_id
GROUP BY oi.user_id;

drop table if exists tmp.tmp_vova_buyer_app_version;
create table tmp.tmp_vova_buyer_app_version as
select datasource,
       buyer_id,
       device_id,
       app_version,
       max_collector_time as last_start_up_date
from (select buyer_id,
             device_id,
             datasource,
             app_version,
             max_collector_time,
             row_number() over (partition by buyer_id,datasource order by pt desc, max_collector_time desc)        as rank
      from dwd.dwd_vova_fact_start_up su where su.buyer_id > 0) su
where su.rank = 1;

insert overwrite table dim.dim_vova_buyers
SELECT u.reg_site_name   as datasource,
       u.user_id         as buyer_id,
       u.email,
       u.user_name       as buyer_name,
       u.gender,
       u.birthday,
       u.reg_time,
       case
           when u.reg_source in (0, 23, 24, 25) then 'pc'
           when u.reg_source in (21, 22, 26) then 'mob'
           when u.reg_source = 11 then 'ios'
           when u.reg_source = 12 then 'android'
           else 'unknown'
           end           as platform,
       u.reg_page,
       u.country         as region_id,
       r.region_code,
       u.language_id,
       l.code            as language_code,
       u.reg_recommender as reg_method,
       u.reg_site_host,
       fp.first_order_id,
       fp.first_order_time,
       fp.first_pay_time,
       fr.first_refund_time,
       oe.ext_value as user_age_group,
       bv.device_id as current_device_id,
       bv.app_version as current_app_version,
       last_start_up_date,
       u.bind_time
from ods_vova_vts.ods_vova_users as u
         left join ods_vova_vts.ods_vova_region r on r.region_id = u.country
         left join ods_vova_vts.ods_vova_languages l on l.languages_id = u.language_id
         left join tmp.tmp_vova_buyer_first_pay fp on fp.buyer_id = u.user_id
         left join tmp.tmp_vova_buyer_first_refund fr on fr.buyer_id = u.user_id
         left join ods_vova_vts.ods_vova_users_extension oe on oe.user_id = u.user_id and oe.ext_name = 'user_age_group'
         left join tmp.tmp_vova_buyer_app_version bv on bv.buyer_id = u.user_id and bv.datasource = u.reg_site_name;
"
spark-sql --conf "spark.app.name=dim_vova_buyers"  --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


