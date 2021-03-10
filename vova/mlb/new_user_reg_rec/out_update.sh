#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

# shellcheck disable=SC2006
table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="mlb_vova_new_user_reg_rec_output_req7832_chenkai_${cur_date}"

###逻辑sql
sql="
-- 表 mlb_vova_rec_m_nurecallad_nb_d
INSERT overwrite TABLE mlb.mlb_vova_rec_m_nurecallad_nb_d PARTITION (pt = '${cur_date}')
SELECT  cluster_key
       ,goods_id
       ,rank_num
FROM
(
	SELECT  *
	       ,row_number() OVER (PARTITION BY cluster_key ORDER BY gmv_cvr DESC,add_cat_cnt DESC,cr_rate_15d DESC,smooth_ctr DESC ) AS rank_num
	       ,COUNT(1) OVER (PARTITION BY cluster_key)                                                                              AS group_cnt
	FROM
	(
		SELECT  t1.*
		       ,t2.cr_rate_15d
		       ,t2.clk_rate_15d                                                                                AS smooth_ctr
		       ,CASE WHEN t2.cr_rate_15d >= 1.5 AND t2.cr_rate_15d <= 10 THEN cast((t1.gmv*t2.cr_rate_15d)/100 AS decimal(14,4))
		             WHEN t2.cr_rate_15d > 10 THEN cast((10*t1.gmv)/100           AS decimal(14,4)) ELSE 0 END AS gmv_cvr
		FROM
		(
			SELECT  CONCAT_WS('&',user_age_group,region_id,gender,platform) AS cluster_key
			       ,t.*
			FROM
			(
				SELECT  *
				FROM mlb.mlb_vova_rec_new_user_d
				WHERE pt = '${cur_date}'
				AND gmv>0
				AND click_uv>=5
				AND platform != 'mob'
			) t
		) t1
		LEFT JOIN
		(
			SELECT  *
			FROM ads.ads_vova_goods_portrait
			WHERE pt = '${cur_date}'
		) t2
		ON t1.goods_id = t2.gs_id
		INNER JOIN
		(
			SELECT  *
			FROM dim.dim_vova_goods
			WHERE is_on_sale = 1
			AND brand_id = 0
		) dg
		ON t1.goods_id = dg.goods_id
	) tt
)
WHERE rank_num <= 500
AND group_cnt >= 8
;


-- 表 mlb_vova_rec_m_nurecallad_d
INSERT overwrite TABLE mlb.mlb_vova_rec_m_nurecallad_d PARTITION (pt = '${cur_date}')
SELECT  cluster_key
       ,goods_id
       ,rank_num
FROM
(
	SELECT  *
	       ,row_number() OVER (PARTITION BY cluster_key ORDER BY gmv_cvr DESC,add_cat_cnt DESC,cr_rate_15d DESC,smooth_ctr DESC ) AS rank_num
	       ,COUNT(1) OVER (PARTITION BY cluster_key)                                                                              AS group_cnt
	FROM
	(
		SELECT  t1.*
		       ,t2.cr_rate_15d
		       ,t2.clk_rate_15d                                                                                AS smooth_ctr
		       ,CASE WHEN t2.cr_rate_15d >= 1.5 AND t2.cr_rate_15d <= 10 THEN cast((t1.gmv*t2.cr_rate_15d)/100 AS decimal(14,4))
		             WHEN t2.cr_rate_15d > 10 THEN cast((10*t1.gmv)/100           AS decimal(14,4)) ELSE 0 END AS gmv_cvr
		FROM
		(
			SELECT  CONCAT_WS('&',user_age_group,region_id,gender,platform) AS cluster_key
			       ,t.*
			FROM
			(
				SELECT  *
				FROM mlb.mlb_vova_rec_new_user_d
				WHERE pt = '${cur_date}'
				AND gmv>0
				AND click_uv>=5
				AND platform != 'mob'
			) t
		) t1
		LEFT JOIN
		(
			SELECT  *
			FROM ads.ads_vova_goods_portrait
			WHERE pt = '${cur_date}'
		) t2
		ON t1.goods_id = t2.gs_id
		INNER JOIN
		(
			SELECT  *
			FROM dim.dim_vova_goods
			WHERE is_on_sale = 1
		) dg
		ON t1.goods_id = dg.goods_id
	) tt
)
WHERE rank_num <= 500
AND group_cnt >= 8
;

-- 表 mlb_vova_rec_m_nurecallreg_nb_d
INSERT overwrite TABLE mlb.mlb_vova_rec_m_nurecallreg_nb_d PARTITION (pt = '${cur_date}')
SELECT  region_id
       ,goods_id
       ,rank_num
FROM
(
	SELECT  *
	       ,row_number() OVER (PARTITION BY region_id ORDER BY gmv_cvr DESC,add_cat_cnt DESC,cr_rate_15d DESC,clk_rate_15d DESC) AS rank_num
	FROM
	(
		SELECT  t1.*
		       ,t3.cr_rate_15d
		       ,t3.gs_gender
		       ,t3.clk_rate_15d
		       ,CASE WHEN t3.cr_rate_15d >= 1.5 AND t3.cr_rate_15d <= 10 THEN cast((t1.gmv*t3.cr_rate_15d)/100 AS decimal(14,4))
		             WHEN t3.cr_rate_15d > 10 THEN cast((t1.gmv*10)/100           AS decimal(14,4)) ELSE 0 END AS gmv_cvr
		FROM
		(
			SELECT  *
			FROM mlb.mlb_vova_rec_new_user_reg_d
			WHERE pt = '${cur_date}'
			AND expre_cnt > 0
			AND clk_cnt > 0
			AND gmv > 0
			AND region_id != 'unknown'
		) t1
		INNER JOIN
		(
			SELECT  *
			FROM dim.dim_vova_goods
			WHERE is_on_sale = 1
			AND brand_id = 0
		) t2
		ON t1.goods_id = t2.goods_id
		LEFT JOIN
		(
			SELECT  *
			FROM ads.ads_vova_goods_portrait
			WHERE pt = '${cur_date}'
		) t3
		ON t1.goods_id = t3.gs_id
	)
)
WHERE rank_num <=400
;

-- 表 mlb_vova_rec_m_nurecallreg_d
INSERT overwrite TABLE mlb.mlb_vova_rec_m_nurecallreg_d PARTITION (pt = '${cur_date}')
SELECT  region_id
       ,goods_id
       ,rank_num
FROM
(
	SELECT  *
	       ,row_number() OVER (PARTITION BY region_id ORDER BY gmv_cvr DESC,add_cat_cnt DESC,cr_rate_15d DESC,clk_rate_15d DESC) AS rank_num
	FROM
	(
		SELECT  t1.*
		       ,t3.cr_rate_15d
		       ,t3.gs_gender
		       ,t3.clk_rate_15d
		       ,CASE WHEN t3.cr_rate_15d >= 1.5 AND t3.cr_rate_15d <= 10 THEN cast((t1.gmv*t3.cr_rate_15d)/100 AS decimal(14,4))
		             WHEN t3.cr_rate_15d > 10 THEN cast((t1.gmv*10)/100           AS decimal(14,4)) ELSE 0 END AS gmv_cvr
		FROM
		(
			SELECT  *
			FROM mlb.mlb_vova_rec_new_user_reg_d
			WHERE pt = '${cur_date}'
			AND expre_cnt > 0
			AND clk_cnt > 0
			AND gmv > 0
			AND region_id != 'unknown'
		) t1
		INNER JOIN
		(
			SELECT  *
			FROM dim.dim_vova_goods
			WHERE is_on_sale = 1
		) t2
		ON t1.goods_id = t2.goods_id
		LEFT JOIN
		(
			SELECT  *
			FROM ads.ads_vova_goods_portrait
			WHERE pt = '${cur_date}'
		) t3
		ON t1.goods_id = t3.gs_id
	)
)
WHERE rank_num <=400
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

