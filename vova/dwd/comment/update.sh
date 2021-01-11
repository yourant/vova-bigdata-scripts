#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
insert overwrite table dwd.dwd_vova_fact_comment
SELECT 'vova'          AS datasource,
       vgc.comment_id,
       vgc.goods_id,
       vgc.category_id AS cat_id,
       vgc.order_goods_id,
       vgc.user_id     AS buyer_id,
       vgc.title,
       vgc.comment,
       vgc.rating,
       vgc.status,
       vgc.post_datetime AS post_time,
       vgc.type,
       vgc.merchant_id AS mct_id,
       vgc.display_order,
       vgc.language_id,
       vgc.tag
FROM ods_vova_vts.ods_vova_goods_comment vgc;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql  --conf "spark.app.name=dwd_vova_fact_comment" --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi