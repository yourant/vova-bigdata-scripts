#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
select count(*) from als_images.fn_rec_gid_pic_similar;
sql="
insert overwrite table ads.fn_ads_min_price_goods partition(pt='${cur_date}')
select
base_data.goods_id,
min_group.goods_id as min_price_goods_id,
'lowest_price' as strategy,
concat(base_data.cat_id, '_' ,base_data.group_id) as group_number,
min_group.shop_price as min_show_price
from
(
select
r.goods_id,
r.group_id,
fdg.cat_id
from
ods_vova_vbai.ods_vova_fn_rec_gid_pic_similar r
inner join dim.dim_zq_goods fdg on fdg.goods_id = r.goods_id
where fdg.datasource = 'florynight'
) base_data
inner join (
select
t1.group_id,
t1.goods_id,
t1.cat_id,
t1.shop_price
from
(
select
r.group_id,
fdg.goods_id,
fdg.cat_id,
fdg.shop_price,
row_number() over(partition by r.group_id,fdg.cat_id order by fdg.shop_price asc ) rank
from
ods_vova_vbai.ods_vova_fn_rec_gid_pic_similar r
inner join dim.dim_zq_goods fdg on fdg.goods_id = r.goods_id
where fdg.datasource = 'florynight'
) t1
where t1.rank = 1
) min_group on base_data.group_id = min_group.group_id and base_data.cat_id = min_group.cat_id
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=20" --conf "spark.app.name=fn_ads_min_price_goods" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



