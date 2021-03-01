#!/usr/bin/env bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

### 2.定义执行HQL
sql="
INSERT OVERWRITE TABLE dim.dim_zq_goods
SELECT
g.goods_id,
vg.virtual_goods_id,
vg.project_name AS datasource,
nvl(tp.platform, 'NA') AS original_source,
tp.item_id AS source_id,
g.cat_id,
c.cat_name,
if(gp.is_on_sale = 1 and gp.is_display = 1 and gp.is_delete = 0,1,0) AS is_on_sale,
c.first_cat_id,
nvl(c.first_cat_name, 'NA') AS first_cat_name,
c.second_cat_id,
nvl(c.second_cat_name, 'NA') AS second_cat_name,
gp.shop_price,
if(gg.img_original like '/spider%', concat('https://supply-img.vova.com.hk', gg.img_original), gg.img_original) as img_original,
nvl(gp.api_goods_id, 'NA') AS commodity_id,
s.domain_group
from
ods_zq_zsp.ods_zq_goods g
INNER JOIN ods_zq_zsp.ods_zq_goods_project gp on gp.goods_id = g.goods_id
left join ods_zq_zsp.ods_zq_virtual_goods vg on g.goods_id = vg.goods_id
         LEFT JOIN
     (
         SELECT t1.img_original,
                t1.goods_id
         FROM (
                  SELECT gg.img_original,
                         gg.goods_id,
                         row_number() OVER(PARTITION BY gg.goods_id ORDER BY gg.last_update_time DESC ) rank
                  FROM ods_zq_zsp.ods_zq_goods_gallery gg
                  WHERE gg.is_delete = 0 AND gg.is_default = 1
              ) t1
         WHERE t1.rank = 1
     ) gg ON gg.goods_id = g.goods_id
left join ods_gyl_gpt.ods_gyl_product_map tp on tp.commodity_id = gp.api_goods_id
left join dim.dim_zq_category c on c.cat_id = g.cat_id
left join dim.dim_zq_site s on s.datasource = vg.project_name
"
#执行hql
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

