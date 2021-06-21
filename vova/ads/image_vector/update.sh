#!/bin/bash
#指定日期和引擎
###先删除昨日分区，支持重跑
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "$cur_date"
pt=$2
#默认日期为昨天
if [ ! -n "$2" ];then
hive -e "msck repair table ads.ads_vova_image_vector_source;"
if [ $? -ne 0 ];then
  exit 1
fi
max_pt=$(hive -e "show partitions ads.ads_vova_image_vector_source" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
pt=${max_pt:3}
fi
echo "pt=$pt"

sql="
with ads_image_vector_source as (
select
s.vector_id,
s.img_id,
s.goods_id,
t.goods_id t_goods_id,
t.min_price_goods_id,
if(s.goods_id =t.min_price_goods_id,1,0) match,
t.group_number
from ads.ads_vova_image_vector_source s
left join (select goods_id,min_price_goods_id,group_number from ads.ads_vova_min_price_goods_d where pt='$cur_date' and strategy='e') t on s.goods_id = t.goods_id
where s.pt='$pt'
),
ads_image_vector_source_res as (
select
vector_id,
img_id,
goods_id,
t_goods_id,
min_price_goods_id,
match,
group_number from ads_image_vector_source where t_goods_id is null
union all
select
vector_id,
img_id,
goods_id,
t_goods_id,
min_price_goods_id,
match,
group_number
from ads_image_vector_source where match =1
union all
select
vector_id,
img_id,
goods_id,
t_goods_id,
min_price_goods_id,
match,
t.group_number
from ads_image_vector_source t left join (select group_number from ads_image_vector_source  where match =1 group by group_number) t1 on t.group_number = t1.group_number
where t1.group_number is null and t.t_goods_id is not null
)

INSERT overwrite TABLE ads.ads_vova_image_vector_target_d partition(pt='$cur_date')
select
/*+ REPARTITION(1) */
t.vector_id,
t.img_id,
t.goods_id,
t.class_id,
t.img_url,
t.vector_base64,
t.pt event_date,
t.sku_id,
t.cat_id,
nvl(t.first_cat_id,0) first_cat_id,
nvl(t.second_cat_id,0) second_cat_id,
t.brand_id,
0 is_delete,
1 is_on_sale,
0 is_update
from ads.ads_vova_image_vector_source t
join ads_image_vector_source_res t1 on t.vector_id=t1.vector_id
where t.pt='$pt';
"
spark-sql --conf "spark.app.name=ads_vova_image_vector_target_d_zhangyin"  -e "$sql"
if [ $? -ne 0 ];then
   exit 1
fi
