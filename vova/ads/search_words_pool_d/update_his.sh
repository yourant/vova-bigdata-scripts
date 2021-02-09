#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_week=`date -d "6 day ago ${cur_date}" +%Y-%m-%d`

sql="
insert overwrite table ads.ads_vova_search_words_pool_d PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
t3.goods_id,
t3.goods_sn,
t3.query,
t3.goods_count,
t3.query_count
from
(
select
t2.query,
t2.goods_id,
t2.goods_sn,
t2.goods_count,
row_number() over (partition by t2.query order by t2.goods_count desc) row_num,
count(t2.query) over (partition by t2.query) query_count
from
(
select
t1.query,
t1.goods_id,
t1.goods_sn,
count(t1.goods_id) goods_count
from
(
select
lower(trim(regexp_extract(list_uri,'q=(.*)',1))) query,
g.goods_id,
g.goods_sn
from
dwd.dwd_vova_log_goods_click gc
left join dim.dim_vova_goods g on gc.virtual_goods_id = g.virtual_goods_id
where gc.pt>'$pre_week' and gc.pt <='$cur_date' and gc.platform='mob' and gc.dp='vova'
and gc.page_code ='search_result'
) t1
group by t1.query,t1.goods_id,t1.goods_sn
) t2
) t3
where t3.row_num <= 50 and t3.query_count > 20 and query !='' and query is not null and t3.goods_id>0;
"
spark-sql --conf "spark.dynamicAllocation.maxExecutors=100" --conf "spark.app.name=ads_vova_search_words_pool_d_zhangyin" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi