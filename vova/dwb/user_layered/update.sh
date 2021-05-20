#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
spark-sql --conf "spark.app.name=dwb_vova_dau_summary" --conf "spark.dynamicAllocation.maxExecutors=100"   -e "

--获取昨日dau
DROP TABLE IF EXISTS tmp.tmp_vova_yesterday_dau;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_yesterday_dau  STORED AS PARQUETFILE  as
select
buyer_id,platform,region_code
from (select
buyer_id,platform,region_code
from (select
buyer_id,platform,region_code
row_number() over(partition by device_id ORDER BY max_collector_time desc) rn
from dwd.dwd_vova_fact_start_up where start_up_date = '${cur_date}' ) tmp
where tmp.rn = 1) t group by buyer_id,platform,region_code;

--获取访问周期
DROP TABLE IF EXISTS tmp.tmp_vova_user_cycle;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_user_cycle  STORED AS PARQUETFILE  as
select /*+ REPARTITION(4) */ buyer_id,platform,region_code,
(unix_timestamp(max(max_collector_time), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(min(min_collector_time), 'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 / count(1) cycle
from dwd.dwd_vova_fact_start_up
group by buyer_id,platform,region_code;
--获取昨日浏览商品
DROP TABLE IF EXISTS tmp.tmp_vova_yesterday_view;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_yesterday_view  STORED AS PARQUETFILE  as
select
buyer_id,os_type platform,geo_country region_code,
count(distinct virtual_goods_id) cnt
from dwd.dwd_vova_log_goods_impression
where pt = '${cur_date}'
group by buyer_id,os_type,geo_country;
--获取购买周期
DROP TABLE IF EXISTS tmp.tmp_vova_user_buy_cycle;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_user_buy_cycle  STORED AS PARQUETFILE  as
select
/*+ REPARTITION(4) */
buyer_id,platform,region_code,
count(1) cnt,
(max(unix_timestamp(pay_time)) - min(unix_timestamp(pay_time))) / 60 / 60 / 24 / count(1) buy_rate
from dwd.dwd_vova_fact_pay
group by buyer_id,platform,region_code;
--获取连续未登陆
DROP TABLE IF EXISTS tmp.tmp_vova_no_load_7_10;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_no_load_7_10  STORED AS PARQUETFILE  as
select
'${cur_date}' now_date,platform,region_code,
count(1) cnt
from (
select
buyer_id,platform,region_code
from (
select buyer_id,platform,region_code,
max(start_up_date) last_load_date
from (
select
buyer_id,platform,region_code,
start_up_date
from (select
buyer_id,platform,region_code,
start_up_date,
row_number() over(partition by device_id ORDER BY max_collector_time desc) rn
from dwd.dwd_vova_fact_start_up where pt >= date_sub('${cur_date}', 10)) tmp
where tmp.rn = 1) group by buyer_id,platform,region_code) t where last_load_date < date_sub('${cur_date}', 7) group by buyer_id,platform,region_code
) group by platform,region_code;
DROP TABLE IF EXISTS tmp.tmp_vova_no_load_10_15;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_no_load_10_15  STORED AS PARQUETFILE  as
select
'${cur_date}' now_date,platform,region_code,
count(1) cnt
from (
select
buyer_id,platform,region_code
from (
select buyer_id,platform,region_code,
max(start_up_date) last_load_date
from (
select
buyer_id,platform,region_code,
start_up_date
from (select
buyer_id,platform,region_code,
start_up_date,
row_number() over(partition by device_id ORDER BY max_collector_time desc) rn
from dwd.dwd_vova_fact_start_up where pt >= date_sub('${cur_date}', 15)) tmp
where tmp.rn = 1) group by buyer_id,platform,region_code) t where last_load_date < date_sub('${cur_date}', 10) group by buyer_id,platform,region_code
) group by platform,region_code;

DROP TABLE IF EXISTS tmp.tmp_vova_user_result;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_user_result  STORED AS PARQUETFILE  as
select
'${cur_date}' now_date,nvl(nvl(a.platform,'NA'),'all') platform,nvl(nvl(a.region_code,'NA'),'all') region_code,
count(case when b.email like '%vovaopen.com%' and c.buyer_id is null then a.buyer_id else null end) latent_user,  --潜在用户
count(case when b.email like '%vovaopen.com%' and c.buyer_id is not null then a.buyer_id else null end) to_change_user,  --待转化用户
count(case when b.email not like '%vovaopen.com%' and c.buyer_id is null
then case when d.cycle > 14 and e.cnt <= 10 then a.buyer_id
else
case when d.cycle = 0  then a.buyer_id else null end
end
else null end) new_user,  --新用户
count(case when b.email not like '%vovaopen.com%' and c.buyer_id is null
then case when d.cycle > 14 and e.cnt > 10 then a.buyer_id
else
case when d.cycle < 14  then a.buyer_id else null end
end
else null end) active_user,  --活跃用户
count(case when b.email not like '%vovaopen.com%' and c.cnt = 1 then  a.buyer_id  else null end) first_order_user, --首单用户
count(case when b.email not like '%vovaopen.com%' and c.cnt >= 2 and c.buy_rate <= 10 then  a.buyer_id  else null end) loyal_user, --忠诚用户
count(case when b.email not like '%vovaopen.com%' and c.cnt >= 2 and c.buy_rate > 10 and d.cycle <= 14 then  a.buyer_id  else null end) lowBuy_highActive_user, --低复购高活跃
count(case when b.email not like '%vovaopen.com%' and c.cnt >= 2 and c.buy_rate > 10 and d.cycle > 14  then  a.buyer_id  else null end) lowBuy_lowActive_user --低复购低活跃
from tmp.tmp_vova_yesterday_dau a
join dim.dim_vova_buyers b
on a.buyer_id = b.buyer_id
left join tmp.tmp_vova_user_buy_cycle c
on a.buyer_id = c.buyer_id and a.platform = c.platform and a.region_code = c.region_code
join tmp.tmp_vova_user_cycle d
on a.buyer_id = d.buyer_id and a.platform = d.platform and a.region_code = d.region_code
left join tmp.tmp_vova_yesterday_view e
on a.buyer_id = e.buyer_id and a.platform = e.platform and a.region_code = e.region_code
group by cube(nvl(a.platform,'NA'),nvl(a.region_code,'NA'))
;

insert overwrite table dwb.dwb_vova_user_layered_result PARTITION (pt = '${cur_date}')
select
a.now_date, --日期
d.cnt dau,
a.latent_user,  --潜在用户
concat(round(a.latent_user * 100 / d.cnt,2),'%') latent_user_rate,
a.to_change_user,  --待转化用户
concat(round(a.to_change_user * 100 / d.cnt,2),'%') to_change_user_rate,
a.new_user,  --新用户
concat(round(a.new_user * 100 / d.cnt,2),'%') new_user_rate,
a.active_user,  --活跃用户
concat(round(a.active_user * 100 / d.cnt,2),'%') active_user_rate,
a.first_order_user, --首单用户
concat(round(a.first_order_user * 100 / d.cnt,2),'%') first_order_user_rate,
a.loyal_user, --忠诚用户
concat(round(a.loyal_user * 100 / d.cnt,2),'%') loyal_user_rate,
a.lowBuy_highActive_user, --低复购高活跃
concat(round(a.lowBuy_highActive_user * 100 / d.cnt,2),'%') lowBuy_highActive_user_rate,
a.lowBuy_lowActive_user, --低复购低活跃
concat(round(a.lowBuy_lowActive_user * 100 / d.cnt,2),'%') lowBuy_lowActive_user_rate,
b.cnt silent, --沉默用户
concat(round(b.cnt * 100 / d.cnt,2),'%') silent_rate,
c.cnt leave_user, --流失用户
concat(round(b.cnt * 100 / d.cnt,2),'%') leave_rate
from tmp.tmp_vova_user_result a
join tmp.tmp_vova_no_load_7_10 b
on a.now_date = b.now_date and a.platform = b.platform and a.region_code = b.region_code
join tmp.tmp_vova_no_load_10_15 c
on a.now_date = c.now_date and a.platform = c.platform and a.region_code = c.region_code
join (select '${cur_date}' now_date,count(1) cnt from tmp.tmp_vova_yesterday_dau) d
on a.now_date = d.now_date
"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mariadb:aurora://db-logistics-w.gitvv.com:3306/themis_logistics_report \
--username vvreport20210517 --password thuy*at1OhG1eiyoh8she --connection-manager org.apache.sqoop.manager.MySQLManager \
--table dwb_vova_user_layered_result \
--update-key "now_date" \
--update-mode allowinsert \
--hcatalog-database dwb \
--hcatalog-table dwb_vova_user_layered_result \
--fields-terminated-by '\001'

if [ $? -ne 0 ]; then
  echo "${cur_date}错误"
  exit 1
fi
