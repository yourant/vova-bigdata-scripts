#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
echo "$pre_date"
fi
pre_month=`date -d "29 day ago ${pre_date}" +%Y-%m-%d`
echo "$pre_month"

sql="
insert overwrite table ads.ads_vova_search_sort_d PARTITION (pt = '${pre_date}')
select
/*+ REPARTITION(1) */
t1.buyer_id user_id,
cast(t2.first_cat_prefer_1w as string) first_cat_prefer_1w,
cast(t2.second_cat_prefer_1w as string) second_cat_prefer_1w,
t2.second_cat_max_click_1m,
t2.second_cat_max_collect_1m,
t2.second_cat_max_cart_1m,
t2.second_cat_max_order_1m,
cast(t2.brand_prefer_1w as string) brand_prefer_1w,
cast(t2.brand_prefer_his as string) brand_prefer_his,
t2.brand_max_click_1m,
t2.brand_max_collect_1m,
t2.brand_max_cart_1m,
t2.brand_max_order_1m,
t2.price_prefer_1w
from
(
select
buyer_id
from dwd.dwd_vova_fact_start_up
where pt>='$pre_month' and buyer_id>0
group by buyer_id
) t1
left join dws.dws_vova_buyer_portrait t2 on t1.buyer_id = t2.buyer_id
where t2.pt='$pre_date';
"
spark-sql --conf "spark.app.name=ads_vova_search_sort_d_zhangyin"  --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
