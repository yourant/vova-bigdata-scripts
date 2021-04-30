#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
#dependence
#ads_vova_six_rank_mct
sql="
DROP TABLE IF EXISTS tmp.tmp_ads_vova_mct_avg_rate_7d;
CREATE TABLE tmp.tmp_ads_vova_mct_avg_rate_7d
SELECT impre.mct_id,
       impre.first_cat_id,
       round(avg(goods_impression_uv), 6)           AS goods_impression_uv_avg_7d,
       sum(paid_uv) AS paid_uv_7d,
       sum(goods_impression_uv) AS goods_impression_uv_7d,
       round(sum(paid_uv) / sum(goods_impression_uv), 6) AS avg_rate_7d
FROM (
         SELECT dvg.mct_id,
                dvg.first_cat_id,
                log.pt,
                count(DISTINCT log.device_id) AS goods_impression_uv
         FROM dwd.dwd_vova_log_goods_impression log
                  INNER JOIN dim.dim_vova_goods dvg ON dvg.virtual_goods_id = log.virtual_goods_id
         WHERE log.datasource = 'vova'
           AND log.platform = 'mob'
           AND log.pt >= date_sub('${cur_date}', 6)
           AND log.pt <= '${cur_date}'
         GROUP BY log.pt, dvg.mct_id, dvg.first_cat_id
     ) impre
         LEFT JOIN
     (
         SELECT dvg.mct_id,
                dvg.first_cat_id,
                date(fp.pay_time)           AS pay_date,
                count(DISTINCT fp.buyer_id) AS paid_uv
         FROM dwd.dwd_vova_fact_pay fp
                  INNER JOIN dim.dim_vova_goods dvg ON dvg.goods_id = fp.goods_id
         WHERE fp.datasource = 'vova'
           AND fp.from_domain LIKE '%api%'
           AND date(fp.pay_time) >= date_sub('${cur_date}', 6)
           AND date(fp.pay_time) <= '${cur_date}'
         GROUP BY dvg.mct_id, dvg.first_cat_id, date(fp.pay_time)
     ) paid ON impre.mct_id = paid.mct_id AND impre.first_cat_id = paid.first_cat_id AND impre.pt = paid.pay_date
GROUP BY impre.mct_id, impre.first_cat_id
;

INSERT OVERWRITE TABLE ads.ads_vova_six_rank_mct_poll_his PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(1) */
       srm.mct_id,
       srm.mct_name,
       srm.first_cat_id,
       srm.add_date,
       ar.goods_impression_uv_avg_7d,
       ar.avg_rate_7d,
       avg_rate.avg_rate_first_cat_7d,
       CASE
           WHEN srm.add_date > date_sub('${cur_date}', 6) THEN 0
           WHEN ar.goods_impression_uv_avg_7d > 10000 AND ar.avg_rate_7d < avg_rate.avg_rate_first_cat_7d THEN 1
           ELSE 0 END AS is_delete
FROM ads.ads_vova_six_rank_mct srm
         INNER JOIN tmp.tmp_ads_vova_mct_avg_rate_7d ar ON srm.mct_id = ar.mct_id AND srm.first_cat_id = ar.first_cat_id
         LEFT JOIN (
    SELECT ar.first_cat_id,
           avg(ar.avg_rate_7d) AS avg_rate_first_cat_7d
    FROM tmp.tmp_ads_vova_mct_avg_rate_7d ar
             INNER JOIN ads.ads_vova_mct_rank mr ON mr.mct_id = ar.mct_id AND mr.first_cat_id = ar.first_cat_id
    WHERE mr.rank = 5
      AND mr.pt = '${cur_date}'
      AND ar.avg_rate_7d > 0
    GROUP BY ar.first_cat_id
) avg_rate ON srm.first_cat_id = avg_rate.first_cat_id
;

INSERT OVERWRITE TABLE ads.ads_vova_six_rank_mct_poll_his PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
six.
from
ads.ads_vova_six_rank_mct_poll_his six
WHERE pt = '${cur_date}'
;

"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=10" \
--conf "spark.dynamicAllocation.maxExecutors=50" \
--conf "spark.app.name=ads_vova_six_rank_mct_poll_his" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
