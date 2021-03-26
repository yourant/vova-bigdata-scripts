#!/bin/bash
#指定日期和引擎
#sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis_2.sh --db_code=vts --table_name=merchant_status_log --etl_type=INIT  --period_type=day --partition_num=3
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

cur_week=`date -d "$cur_date" +%w`
echo $cur_date
echo $cur_week
if [ "$cur_week" != 6 ];then
exit 0
fi

###逻辑sql

query_sql="
SELECT dm.spsor_name           AS spsor_name,
       dm.mct_name,
       dm.mct_id               AS mct_id,
       dm.first_publish_time   AS first_publish_time,
       mct_log.audit_pass_time AS audit_pass_time,
       CASE
           WHEN dm.review_status = 'passed' AND dm.is_delete = 0 THEN 'passed'
           WHEN dm.review_status = 'not_passed' AND dm.is_delete = 0 THEN 'not_passed'
           WHEN dm.is_delete = 1 THEN 'delete'
           WHEN dm.review_status = 'to_review' AND dm.is_delete = 0 THEN 'to_review'
           WHEN dm.review_status = 'to_complete' AND dm.is_delete = 0 THEN 'to_complete'
           ELSE 'normal' END   AS merchant_status,
       fp.sale_goods_cnt       AS sale_goods_cnt,
       goods.on_sale_goods_cnt AS on_sale_goods_cnt,
       fp.order_cnt            AS order_cnt,
       fp.gmv                  AS gmv,
       main_cat.cat_name
FROM dim.dim_vova_merchant dm
         LEFT JOIN (SELECT sum(fp.shop_price * fp.goods_number + fp.shipping_fee) AS gmv,
                           count(DISTINCT fp.order_goods_id)                      AS order_cnt,
                           sum(fp.goods_number)                                   AS sale_goods_cnt,
                           mct_id
                    FROM dwd.dwd_vova_fact_pay fp
                    WHERE date(fp.pay_time) >= trunc('${cur_date}', 'MM')
                      AND date(fp.pay_time) <= '${cur_date}'
                    GROUP BY mct_id
) fp ON dm.mct_id = fp.mct_id
         LEFT JOIN (
    SELECT count(goods_id) AS on_sale_goods_cnt,
           mct_id
    FROM dim.dim_vova_goods g
    WHERE g.is_on_sale = 1
    GROUP BY mct_id
) AS goods ON dm.mct_id = goods.mct_id
         LEFT JOIN
     (
         SELECT merchant_id,
                min(create_time) AS audit_pass_time
         FROM ods_vova_vts.ods_vova_merchant_status_log
         WHERE new_value = 'passed'
         GROUP BY merchant_id
     ) mct_log ON mct_log.merchant_id = dm.mct_id
         LEFT JOIN (
    SELECT mct_id,
           c.cat_name
    FROM (
             SELECT mct_id,
                    first_cat_id,
                    row_number() OVER(PARTITION BY mct_id ORDER BY order_cnt DESC) AS rank
             FROM
                 (
                 SELECT COUNT (DISTINCT dog.order_goods_id) AS order_cnt,
                 dog.mct_id,
                 dg.first_cat_id
                 FROM dim.dim_vova_order_goods dog
                 INNER JOIN dim.dim_vova_goods dg on dg.goods_id = dog.goods_id
                 WHERE date(dog.pay_time) >= date_sub('${cur_date}', 29)
                 AND date(dog.pay_time) <= '${cur_date}'
                 AND dog.pay_status >= 1
                 AND dog.sku_shipping_status >= 1
                 GROUP BY dog.mct_id, dg.first_cat_id
                 ) t1
         ) t2
             LEFT JOIN dim.dim_vova_category c ON c.cat_id = t2.first_cat_id
    WHERE t2.rank = 1
) main_cat ON dm.mct_id = main_cat.mct_id
"

head="
spsor_name,
mct_name,
mct_id,
first_publish_time,
audit_pass_time,
merchant_status,
本月销量,
当前在架商品数,
本月支付子订单数,
本月gmv,
主营一级类目
;
"


spark-submit \
--deploy-mode client \
--name 'dwb_vova_bd_email' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "${query_sql}"  \
-head "${head}"  \
-receiver "ethan.zheng@i9i8.com" \
-title "商家本月gmv统计(${cur_date})" \
--type attachment \
--fileName "商家本月gmv统计(${cur_date})"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "发送邮件失败"
  exit 1
fi

