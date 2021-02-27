#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
ALTER TABLE ads.ads_vova_buyer_portrait_goods_likes DROP if exists partition(pt = '$(date -d "${pre_date:0:10} -60day" +%Y-%m-%d)');
INSERT overwrite TABLE ads.ads_vova_buyer_portrait_goods_likes partition(pt='$pre_date')
SELECT
/*+ REPARTITION(30) */
/*+ BROADCAST(tmp_ord) */
	tmp_behave.buyer_id,
	tmp_behave.gs_id,
	tmp_behave.expre_cnt_1w,
	tmp_behave.clk_cnt_1m,
	tmp_behave.clk_valid_cnt_2m,
	tmp_behave.collect_cnt_2m,
	nvl(tmp_ord.ord_cnt,0),
	1+clk_cnt_1m*power(e(),-0.15*1)+collect_cnt_2m*power(e(),-0.1*1)+collect_cnt_2m*power(e(),-0.1*1)+2*nvl(tmp_ord.ord_cnt,0)*power(e(),-0.05*1)+0.5*expre_cnt_1w*power(e(),-0.4*1) as rating
FROM
	(
SELECT
	buyer_id,
	gs_id,
	sum( IF ( day_gap < 7, expre_cnt, 0 ) ) AS expre_cnt_1w,
	sum( IF ( day_gap < 30, clk_cnt, 0 ) ) AS clk_cnt_1m,
	sum( IF ( day_gap < 60, clk_valid_cnt, 0 ) ) AS clk_valid_cnt_2m,
	sum( IF ( day_gap < 60, collect_cnt, 0 ) ) AS collect_cnt_2m
FROM
	(
SELECT
	buyer_id,
	gs_id,
	pt,
	datediff( '${pre_date}', pt ) AS day_gap,
	sum( expre_cnt ) AS expre_cnt,
	sum( clk_cnt ) AS clk_cnt,
	sum( clk_valid_cnt ) AS clk_valid_cnt,
	sum( collect_cnt ) AS collect_cnt,
	sum( add_cat_cnt ) AS add_cat_cnt,
	sum( ord_cnt ) AS ord_cnt
FROM
	dws.dws_vova_buyer_goods_behave
WHERE
	pt > date_sub( '${pre_date}', 60 )
	AND pt <= '${pre_date}'
GROUP BY
	buyer_id,
	gs_id,
	pt
	)
GROUP BY
	buyer_id,
	gs_id
	) tmp_behave
	LEFT JOIN (
SELECT
	buyer_id,
	goods_id gs_id,
	count( * ) AS ord_cnt
FROM
	dwd.dwd_vova_fact_pay fp
WHERE
	to_date ( order_time ) > date_sub( '${pre_date}', 180 )
	AND to_date ( order_time ) <= '${pre_date}'
	AND fp.platform IN ( 'ios', 'android' )
GROUP BY
	buyer_id,
	goods_id
	) tmp_ord ON tmp_behave.buyer_id = tmp_ord.buyer_id
	AND tmp_behave.gs_id = tmp_ord.gs_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--driver-memory 8G \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=ads_vova_buyer_portrait_goods_likes" \
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
if [ $? -ne 0 ]; then
  exit 1
fi
