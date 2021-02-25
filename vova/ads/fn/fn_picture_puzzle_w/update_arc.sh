#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
echo "$cur_date"

#img_original = 'https://supply-img.vova.com.hk/spider/images/item/6a/09/f68e033b62c364a1ae3845401cb26a09.jpg'
#img_original = '/spider/images/item/e8/1d/24c3c8e06fce27b61830c15c3341e81d.jpg'

sql="

DROP TABLE IF EXISTS tmp.tmp_fn_ads_picture_puzzle_step1;
CREATE TABLE tmp.tmp_fn_ads_picture_puzzle_step1 AS
SELECT img_id,
       goods_id,
       img_color,
       img_url,
       img_original,
       is_default,
       rank AS location_id,
       datasource
FROM (
         SELECT g.datasource,
                gg.img_id,
                gg.goods_id,
                gg.img_color,
                gg.img_url,
                gg.img_original,
                gg.is_default,
                row_number() OVER(PARTITION BY g.datasource, gg.goods_id ORDER BY gg.last_update_time DESC) rank
         FROM
             ods_zq_zsp.ods_zq_goods_gallery gg
             INNER JOIN dim.dim_zq_goods g ON gg.goods_id = g.goods_id
         WHERE g.is_on_sale = 1
             AND gg.is_default = 1
             AND g.datasource='florynight'
     ) t1
WHERE t1.rank = 1

UNION ALL

SELECT img_id,
       goods_id,
       img_color,
       img_url,
       img_original,
       is_default,
       rank3 + 1 AS location_id,
       datasource
FROM (
         SELECT datasource,
                img_id,
                goods_id,
                img_color,
                img_url,
                img_original,
                is_default,
                row_number() OVER (PARTITION BY datasource, goods_id ORDER BY sequence) AS rank3
         FROM (
                  SELECT datasource,
                         img_id,
                         goods_id,
                         img_color,
                         img_url,
                         img_original,
                         is_default,
                         sequence,
                         row_number() OVER (PARTITION BY datasource, goods_id, img_color ORDER BY sequence) rank2
                  FROM (
                           SELECT g.datasource,
                                  gg.img_id,
                                  gg.goods_id,
                                  gg.img_color,
                                  gg.img_url,
                                  gg.img_original,
                                  gg.is_default,
                                  gg.sequence,
                                  row_number()
                                          OVER (PARTITION BY g.datasource, gg.goods_id, gg.img_original ORDER BY gg.sequence) rank
                           FROM ods_zq_zsp.ods_zq_goods_gallery gg
                                    INNER JOIN dim.dim_zq_goods g ON gg.goods_id = g.goods_id
                           WHERE g.is_on_sale = 1
                             AND gg.is_default = 0
                             AND g.datasource = 'florynight'
                       ) t1
                  WHERE t1.rank = 1
              ) t2
         WHERE t2.rank2 = 1
     ) t3
WHERE t3.rank3 <= 3

;

DROP TABLE IF EXISTS tmp.tmp_fn_ads_picture_puzzle_step2;
CREATE TABLE tmp.tmp_fn_ads_picture_puzzle_step2 AS
SELECT g.datasource,
       gg.img_id,
       gg.goods_id,
       gg.img_color,
       gg.img_url,
       gg.img_original,
       gg.is_default,
       gg.sequence
FROM ods_zq_zsp.ods_zq_goods_gallery gg
         INNER JOIN dim.dim_zq_goods g ON gg.goods_id = g.goods_id
         LEFT JOIN tmp.tmp_fn_ads_picture_puzzle_step1 t1 on t1.img_id = gg.img_id
                           WHERE g.is_on_sale = 1
                             AND gg.is_default = 0
                             AND g.datasource = 'florynight'
                             AND t1.img_id is null
;


DROP TABLE IF EXISTS tmp.tmp_fn_ads_picture_puzzle_step3;
CREATE TABLE tmp.tmp_fn_ads_picture_puzzle_step3 AS
select
datasource,
img_id,
goods_id,
img_color,
img_url,
img_original,
is_default,
cnt + rank AS location_id
from
(
select
t1.datasource,
t1.goods_id,
t1.cnt,
t2.img_id,
t2.img_color,
t2.img_url,
t2.img_original,
t2.is_default,
t2.sequence,
row_number() OVER (PARTITION BY t2.datasource, t2.goods_id ORDER BY t2.sequence) AS rank
from
(
SELECT count(*) as cnt,
       goods_id,
       datasource
FROM tmp.tmp_fn_ads_picture_puzzle_step1
GROUP BY goods_id, datasource
having cnt < 4
) t1
inner join tmp.tmp_fn_ads_picture_puzzle_step2 t2 on t2.goods_id = t1.goods_id AND t2.datasource = t1.datasource
) fin
where fin.rank <= 4 - cnt
;

INSERT OVERWRITE TABLE ads.ads_zq_fn_picture_puzzle_v2_w PARTITION (pt = '${cur_date}')
select
t1.img_id,
t1.goods_id,
fdg.virtual_goods_id,
fdg.first_cat_id,
replace(fdg.first_cat_name, ',', '') first_cat_name,
fdg.second_cat_id,
replace(fdg.second_cat_name, ',', '') second_cat_name,
replace(t1.img_color, ',', '') img_color,
t1.img_url,
if(t1.img_original like '/spider%', concat('https://supply-img.vova.com.hk', t1.img_original), t1.img_original) as img_original,
t1.is_default,
t1.location_id,
t1.datasource
FROM (
SELECT datasource,
       img_id,
       goods_id,
       img_color,
       img_url,
       img_original,
       is_default,
       location_id
         from
         tmp.tmp_fn_ads_picture_puzzle_step1
         UNION ALL
SELECT datasource,
       img_id,
       goods_id,
       img_color,
       img_url,
       img_original,
       is_default,
       location_id
                from
         tmp.tmp_fn_ads_picture_puzzle_step3
     ) t1
     INNER JOIN dim.dim_zq_goods fdg on fdg.goods_id = t1.goods_id AND fdg.datasource = t1.datasource
;
INSERT OVERWRITE TABLE ads.ads_zq_fn_picture_puzzle_v2
select
/*+ REPARTITION(1) */
img_id,
goods_id,
virtual_goods_id,
first_cat_id,
first_cat_name,
second_cat_id,
second_cat_name,
img_color,
img_url,
img_original,
is_default,
location_id,
datasource
from ads.ads_zq_fn_picture_puzzle_v2_w where pt='${cur_date}'
;

"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=20" --conf "spark.app.name=ads_zq_fn_picture_puzzle_w" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

