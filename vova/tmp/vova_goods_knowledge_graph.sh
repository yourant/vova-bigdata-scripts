#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
with tmp_result as (
SELECT  t.pt 
       ,t.test_group 
       ,t.cat_name 
       ,SUM(t.expre_pv)                                                    AS expre_pv 
       ,SUM(t.expre_uv)                                                    AS expre_uv 
       ,SUM(t.click_pv)                                                    AS click_pv 
       ,SUM(t.click_uv)                                                    AS click_uv 
       ,SUM(t.cart_pv)                                                     AS cart_pv 
       ,SUM(t.cart_uv)                                                     AS cart_uv 
       ,SUM(t.order_cnt)                                                   AS order_cnt 
       ,SUM(t.pay_cnt)                                                     AS pay_cnt 
       ,SUM(t.gmv)                                                         AS gmv 
       ,concat(nvl(round(SUM(t.cart_uv) * 100 / SUM(t.expre_uv),2),0),'%') AS cart_rate 
       ,concat(round(SUM(t.click_pv) * 100 / SUM(t.expre_pv),2),'%')       AS ctr 
       ,concat(nvl(round(SUM(t.pay_uv) * 100 / SUM(t.expre_uv),3),0),'%')  AS cr 
       ,nvl(round(SUM(t.gmv) *100 / SUM(t.expre_uv),6),0)                  AS gmv_cr
FROM 
(
	SELECT  a.pt 
	       ,a.test_group 
	       ,(case WHEN b.first_cat_id in(5768,194,5977,5713,5777,5773) THEN b.first_cat_name WHEN b.second_cat_id in(5743,5954,5741) THEN b.first_cat_name else '' end) AS cat_name 
	       ,COUNT(1)                  AS expre_pv 
	       ,COUNT(distinct device_id) AS expre_uv 
	       ,0                         AS click_pv 
	       ,0                         AS click_uv 
	       ,0                         AS cart_pv 
	       ,0                         AS cart_uv 
	       ,0                         AS pay_uv 
	       ,0                         AS order_cnt 
	       ,0                         AS pay_cnt 
	       ,0                         AS gmv
	FROM 
	(
		SELECT  pt 
		       ,device_id 
		       ,cast(get_json_object(extra,'$.current_goods') AS bigint) AS vir_goods_id 
		       ,nvl(if(test_info like '%&rec_detail_recall_vga%','vga',if(test_info like '%&rec_detail_recall_vgd%','vgd','others')),'all') AS test_group
		FROM dwd.dwd_vova_log_impressions_arc
		WHERE pt='${cur_date}' 
		AND event_type='goods' 
		AND datasource='vova' 
		AND page_code='product_detail' 
		AND test_info like '%&rec_detail_recall_%' 
		AND get_json_object(extra, '$.current_goods') is not null  
	) a
	INNER JOIN 
	(
		SELECT  virtual_goods_id 
		       ,first_cat_name
		       ,first_cat_id 
		       ,second_cat_id
		FROM dim.dim_vova_goods
		WHERE first_cat_id in(5768,194,5977,5713,5777,5773) or second_cat_id in(5743,5954,5741)  
	) b
	ON a.vir_goods_id=b.virtual_goods_id
	GROUP BY  a.pt 
	         ,a.test_group 
	         ,(case WHEN b.first_cat_id in(5768,194,5977,5713,5777,5773) THEN b.first_cat_name WHEN b.second_cat_id in(5743,5954,5741) THEN b.first_cat_name else '' end) 
	UNION ALL
	 -- 点击pv、uv
	SELECT  a.pt 
	       ,a.test_group 
	       ,(case WHEN b.first_cat_id in(5768,194,5977,5713,5777,5773) THEN b.first_cat_name WHEN b.second_cat_id in(5743,5954,5741) THEN b.first_cat_name else '' end) AS cat_name 
	       ,0                         AS expre_pv 
	       ,0                         AS expre_uv 
	       ,COUNT(1)                  AS click_pv 
	       ,COUNT(distinct device_id) AS click_uv 
	       ,0                         AS cart_pv 
	       ,0                         AS cart_uv 
	       ,0                         AS pay_uv 
	       ,0                         AS order_cnt 
	       ,0                         AS pay_cnt 
	       ,0                         AS gmv
	FROM 
	(
		SELECT  pt 
		       ,nvl(if(test_info like '%&rec_detail_recall_vga%','vga',if(test_info like '%&rec_detail_recall_vgd%','vgd','others')),'all') test_group 
		       ,device_id 
		       ,virtual_goods_id
		FROM dwd.dwd_vova_log_goods_click a
		WHERE pt = '${cur_date}' 
		AND page_code = 'product_detail' 
		AND test_info like '%&rec_detail_recall_%' 
		AND datasource = 'vova'  
	) a
	INNER JOIN 
	(
		SELECT  virtual_goods_id 
		       ,first_cat_id 
		        ,first_cat_name
		       ,second_cat_id
		FROM dim.dim_vova_goods
		WHERE first_cat_id in(5768,194,5977,5713,5777,5773) or second_cat_id in(5743,5954,5741)  
	) b
	ON a.virtual_goods_id=b.virtual_goods_id
	GROUP BY  a.pt 
	         ,a.test_group 
	         ,(case WHEN b.first_cat_id in(5768,194,5977,5713,5777,5773) THEN b.first_cat_name WHEN b.second_cat_id in(5743,5954,5741) THEN b.first_cat_name else '' end) 
	UNION ALL
	 -- 加购uv
	SELECT  a.pt 
	       ,a.test_group 
	       ,(case WHEN b.first_cat_id in(5768,194,5977,5713,5777,5773) THEN b.first_cat_name WHEN b.second_cat_id in(5743,5954,5741) THEN b.first_cat_name else '' end) AS cat_name 
	       ,0                         AS expre_pv 
	       ,0                         AS expre_uv 
	       ,0                         AS click_pv 
	       ,0                         AS click_uv 
	       ,COUNT(1)                  AS cart_pv 
	       ,COUNT(distinct device_id) AS cart_uv 
	       ,0                         AS pay_uv 
	       ,0                         AS order_cnt 
	       ,0                         AS pay_cnt 
	       ,0                         AS gmv
	FROM 
	(
		SELECT  pt 
		       ,nvl(if(pre_test_info like '%&rec_detail_recall_vga%','vga',if(pre_test_info like '%&rec_detail_recall_vgd%','vgd','others')),'all') test_group 
		       ,device_id 
		       ,virtual_goods_id
		FROM dwd.dwd_vova_fact_cart_cause_v2
		WHERE pt = '${cur_date}' 
		AND pre_page_code = 'product_detail' 
		AND pre_test_info like '%&rec_detail_recall_%' 
		AND datasource = 'vova'  
	) a
	INNER JOIN 
	(
		SELECT  virtual_goods_id 
		       ,first_cat_name
		       ,first_cat_id 
		       ,second_cat_id
		FROM dim.dim_vova_goods
		WHERE first_cat_id in(5768,194,5977,5713,5777,5773) or second_cat_id in(5743,5954,5741)  
	) b
	ON a.virtual_goods_id=b.virtual_goods_id
	GROUP BY  a.pt 
	         ,a.test_group 
	         ,(case WHEN b.first_cat_id in(5768,194,5977,5713,5777,5773) THEN b.first_cat_name WHEN b.second_cat_id in(5743,5954,5741) THEN b.first_cat_name else '' end) 
	UNION ALL
	 -- // 支付uv 订单数 gmv
	SELECT  a.pt 
	       ,a.test_group 
	       ,(case WHEN b.first_cat_id in(5768,194,5977,5713,5777,5773) THEN b.first_cat_name WHEN b.second_cat_id in(5743,5954,5741) THEN b.first_cat_name else '' end) AS cat_name 
	       ,0                                                                                      AS expre_pv 
	       ,0                                                                                      AS expre_uv 
	       ,0                                                                                      AS click_pv 
	       ,0                                                                                      AS click_uv 
	       ,0                                                                                      AS cart_pv 
	       ,0                                                                                      AS cart_uv 
	       ,COUNT(distinct a.device_id)                                                            AS pay_uv 
	       ,COUNT(distinct a.order_goods_id)                                                       AS order_cnt 
	       ,COUNT(distinct (if(c.order_goods_id is not null,a.order_goods_id,null)))               AS pay_cnt 
	       ,SUM(if(c.order_goods_id is not null,c.goods_number * c.shop_price + c.shipping_fee,0)) AS gmv
	FROM 
	(
		SELECT  pt 
		       ,nvl(if(pre_test_info like '%&rec_detail_recall_vga%','vga',if(pre_test_info like '%&rec_detail_recall_vgd%','vgd','others')),'all') test_group 
		       ,device_id 
		       ,order_goods_id
		FROM dwd.dwd_vova_fact_order_cause_v2
		WHERE pt = '${cur_date}' 
		AND pre_page_code = 'product_detail' 
		AND pre_test_info like '%&rec_detail_recall_%' 
		AND datasource = 'vova'  
	) a
	INNER JOIN 
	(
		SELECT  virtual_goods_id 
		        ,first_cat_name
		       ,first_cat_id 
		       ,second_cat_id
		FROM dim.dim_vova_goods
		WHERE first_cat_id in(5768,194,5977,5713,5777,5773) or second_cat_id in(5743,5954,5741)  
	) b
	ON a.order_goods_id=b.virtual_goods_id
	LEFT JOIN dwd.dwd_vova_fact_pay c
	ON a.order_goods_id = c.order_goods_id
	GROUP BY  a.pt 
	         ,a.test_group 
	         ,(case WHEN b.first_cat_id in(5768,194,5977,5713,5777,5773) THEN b.first_cat_name WHEN b.second_cat_id in(5743,5954,5741) THEN b.first_cat_name else '' end) 
) t
WHERE t.cat_name!='' 
AND t.cat_name is not null 
GROUP BY  t.pt 
         ,t.test_group 
         ,t.cat_name
)

--
insert overwrite table tmp.tmp_vova_goods_knowledge_graph partition(pt='${cur_date}')
select /*+ REPARTITION(1) */ test_group ,cat_name ,expre_pv ,expre_uv ,click_pv ,click_uv ,cart_pv ,cart_uv ,order_cnt ,pay_cnt ,gmv ,cart_rate ,ctr ,cr ,gmv_cr from tmp_result;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.dynamicAllocation.maxExecutors=150" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi