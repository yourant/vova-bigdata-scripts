#!/bin/bash

cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

###更新goods维度
sql="
insert overwrite table dim.dim_vova_goods
select 'vova' as datasource,
       g.goods_id,
       vg.virtual_goods_id,
       g.cp_goods_id,
       g.brand_id,
       g.goods_sn,
       g.goods_name,
       g.goods_desc,
       g.sale_status,
       g.keywords,
       g.add_time,
       if(g.is_on_sale=1 and g.is_display=1 and g.is_delete=0,1,0),
       g.is_complete,
       g.is_new,
       g.cat_id,
       c.cat_name,
       c.first_cat_id,
       c.first_cat_name,
       c.second_cat_id,
       c.second_cat_name,
       c.three_cat_id,
       c.three_cat_name as third_cat_name,
       g.merchant_id    as mct_id,
       m.store_name as mct_name,
       g.shop_price,
       g.shipping_fee,
       g.goods_weight,
       got.first_on_time,
       got.first_off_time,
       got.last_on_time,
       got.last_off_time,
       g.goods_thumb
from ods_vova_vts.ods_vova_goods g
         inner join ods_vova_vts.ods_vova_virtual_goods vg on g.goods_id = vg.goods_id
         inner join dim.dim_vova_category c on c.cat_id = g.cat_id
         left join
-- 新增商品（最早、最晚）上线时间、下线时间
(SELECT
      goods_id,
      min( IF ( action = 'on', create_time, NULL ) ) first_on_time,
      min( IF ( action = 'off', create_time, NULL ) ) first_off_time,
      max( IF ( action = 'on', create_time, NULL ) ) last_on_time,
      max( IF ( action = 'off', create_time, NULL ) ) last_off_time
FROM
      ( SELECT goods_id, CASE WHEN action = 'on' THEN 'on' ELSE 'off' END AS action, create_time FROM ods_vova_vts.ods_vova_goods_on_sale_record )
      GROUP BY
      goods_id) got
      on g.goods_id = got.goods_id
  left join ods_vova_vts.ods_vova_merchant m on g.merchant_id = m.merchant_id

"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.app.name=dim_vova_goods" --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.sql.mergeSmallFileSize=10485760"  --conf "spark.sql.broadcastTimeout=36000"  --conf "spark.sql.output.merge=true"  -e "$sql"

#如果脚本失败，则报错

if [ $? -ne 0 ];then
  exit 1
fi

