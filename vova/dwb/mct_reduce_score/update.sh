#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
pre10_day=`date -d "-10 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
with tmp_reduce_score as(
      SELECT
        mct_id,
        red_score,
        today_red_score
      FROM
        (
      SELECT
        merchant_id AS mct_id,
        sum( reduce_score ) AS red_score,
        sum( IF ( to_date ( create_time ) = '${cur_date}', reduce_score, 0 ) ) AS today_red_score
      FROM
        ods_vova_vts.ods_vova_merchant_assessment_score_log
      WHERE
        to_date ( create_time ) <= '${cur_date}' AND to_date ( create_time ) >= '${pre10_day}'
      GROUP BY
        merchant_id
        )
      WHERE
        red_score >=2
)

insert overwrite table dwb.dwb_vova_mct_reduce_score_history  PARTITION (pt = '${cur_date}')
SELECT
  dm.mct_name,
  trs.red_score,
  trs.today_red_score,
  pay_data.gs_sale_cnt,
  pay_data.gmv
FROM
  tmp_reduce_score trs
  INNER JOIN (
    SELECT
        fp.mct_id,
        sum( fp.goods_number ) gs_sale_cnt,
        sum( fp.shop_price * fp.goods_number + fp.shipping_fee ) AS gmv
      FROM
        dwd.dwd_vova_fact_pay fp
      WHERE
        to_date ( pay_time ) <= '${cur_date}' AND to_date ( pay_time ) >= '${pre10_day}'
      GROUP BY
        fp.mct_id ) pay_data
  ON trs.mct_id = pay_data.mct_id
  INNER JOIN dim.dim_vova_merchant dm ON trs.mct_id = dm.mct_id
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 5G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=2" \
--conf "spark.dynamicAllocation.initialExecutors=5" \
--conf "spark.app.name=dwb_vova_mct_reduce_score_history" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 50" \
--conf "spark.sql.shuffle.partitions=50" \
--conf "spark.dynamicAllocation.maxExecutors=20" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

