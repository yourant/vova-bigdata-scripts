#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

sql="
insert overwrite table dwb.dwb_vova_query_translation_quality_control PARTITION (pt='${cur_date}')
select
element_type,
search_cnt,
search_uv,
nvl(count(distinct pay_buyer)/count(distinct expre_buyer),0)*100 as rate,
nvl(count(distinct pay_buyer_46)/count(distinct expre_buyer_46),0)*100 as rate_46,
nvl(count(distinct pay_buyer_12)/count(distinct expre_buyer_12),0)*100 as rate_12,
nvl(count(distinct pay_buyer_32)/count(distinct expre_buyer_32),0)*100 as rate_32,
nvl(count(distinct pay_buyer_50)/count(distinct expre_buyer_50),0)*100 as rate_50,
nvl(count(distinct pay_buyer_36)/count(distinct expre_buyer_36),0)*100 as rate_36
from (
select
a.element_type,
a.buyer_id as expre_buyer,
if(b.pay_cnt>0,b.buyer_id,null) as pay_buyer,
if(get_rp_name(a.recall_pool) like '%46%',a.buyer_id,null) as expre_buyer_46,
if(b.pay_cnt>0 and get_rp_name(a.recall_pool) like '%46%',b.buyer_id,null) as pay_buyer_46,
if(get_rp_name(a.recall_pool) like '%12%',a.buyer_id,null) as expre_buyer_12,
if(b.pay_cnt>0 and get_rp_name(a.recall_pool) like '%12%',b.buyer_id,null) as pay_buyer_12,
if(get_rp_name(a.recall_pool) like '%32%',a.buyer_id,null) as expre_buyer_32,
if(b.pay_cnt>0 and get_rp_name(a.recall_pool) like '%32%',b.buyer_id,null) as pay_buyer_32,
if(get_rp_name(a.recall_pool) like '%50%',a.buyer_id,null) as expre_buyer_50,
if(b.pay_cnt>0 and get_rp_name(a.recall_pool) like '%50%',b.buyer_id,null) as pay_buyer_50,
if(get_rp_name(a.recall_pool) like '%36%',a.buyer_id,null) as expre_buyer_36,
if(b.pay_cnt>0 and get_rp_name(a.recall_pool) like '%36%',b.buyer_id,null) as pay_buyer_36
from dwd.dwd_vova_log_impressions_arc a
left join (
select
dog.buyer_id,
count(*) as pay_cnt
from dim.dim_vova_order_goods dog
where to_date(dog.pay_time) = '${cur_date}'
and dog.pay_status >= 1
group by dog.buyer_id
) b
on b.buyer_id = a.buyer_id
where a.page_code ='search_result'
and a.list_type in ('/search_result','/search_result_recommend')
and a.platform = 'mob'
and a.datasource = 'vova'
and ((a.pt='${cur_date}'and date(a.collector_ts)='${cur_date}' )
or (a.pt=date_sub('${cur_date}',1) and a.hour ='23' and date(a.collector_ts)='${cur_date}')
or (a.pt=date_add('${cur_date}',1) and a.hour ='00' and date(a.collector_ts)='${cur_date}'))
) t
left join (
select element_id,
count(*) as search_cnt,
count(distinct buyer_id) as search_uv
from dwd.dwd_vova_log_common_click
where pt = '${cur_date}'
and page_code = 'search_begin'
and element_name = 'search_confirm'
and platform = 'mob'
and datasource = 'vova'
group by element_id
) c
on c.element_id = t.element_type
where search_cnt >= 5
group by
element_type,
search_cnt,
search_uv
order by rate_46,search_cnt asc
limit 1000
;
"

spark-sql \
--conf "spark.app.name=dwb_vova_query_translation_quality_control" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=10" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi

spark-submit \
--deploy-mode client \
--name 'dwb_vova_query_translation_quality_control' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "select * from dwb.dwb_vova_query_translation_quality_control where pt='${cur_date}'"  \
-head "搜索词,搜索频次,搜索人数,转化率,rp = 46（翻译）转化率,rp = 12（es）转化率,rp = 32（高频搜索词）转化率,rp = 50（语义识别）转化率,rp = 36（意图识别）转化率"  \
-receiver "huachen@vova.com.hk,mulan@vova.com.hk,ruohai@vova.com.hk,deyou@vova.com.hk" \
-title "搜索翻译转化率监控 ${cur_date}" \
--type attachment \
--fileName "搜索翻译转化率监控 ${cur_date}"

if [ $? -ne 0 ];then
  exit 1
fi