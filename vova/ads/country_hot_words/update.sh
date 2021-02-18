#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
pre_week=`date -d "6 day ago ${pre_date}" +%Y-%m-%d`
fi
sql="
with ads_country_hot_search_words as(
select
region_id,
region_code,
hot_words,
rank
from
(
select
r.region_id,
t2.region_code,
t2.hot_words,
row_number() over (partition by r.region_id, t2.region_code order by cnt desc) rank
from
(
select
region_code,
hot_words,
count(*) cnt
from
(
select
geo_country region_code,
lower(trim(element_id)) hot_words
from dwd.dwd_vova_log_common_click
where pt>='$pre_week' and element_name='search_confirm' and geo_country in ('GB','FR','DE','IT','ES')
and element_id is not null and element_id !=''
) t1 group by
region_code,
hot_words
) t2
left join (select distinct region_id,country_code from dim.dim_vova_region where parent_id = 0 and country_code in ('GB','FR','DE','IT','ES')) r on t2.region_code = r.country_code
) t where rank<=10000
)
insert overwrite table ads.ads_vova_country_hot_search_words PARTITION (pt = '$pre_date')
select
/*+ REPARTITION(1) */
region_id,
region_code,
hot_words,
rank
from
(
select
region_id,
region_code,
hot_words,
rank
from ads_country_hot_search_words
where region_code ='FR'
union all
select
region_id,
region_code,
hot_words,
rank
from ads_country_hot_search_words
where region_code !='FR' and rank<=1000
) t
"
spark-sql --conf "spark.app.name=ads_vova_country_hot_search_words_zhangyin"  --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
