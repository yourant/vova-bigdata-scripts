#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
WITH tmp_news_email AS (
SELECT
    id,
    email,
    max( update_time ) AS login_time
FROM
    (
SELECT
    tc.id,
    es.email,
    es.update_time
FROM
    ods.vova_email_send_log_a es
    INNER JOIN ods.vova_email_task_config tc ON es.config_id = tc.id
WHERE
    es.OPEN = 1
    AND tc.app_name = 'florynight'
    AND datediff( es.update_time, tc.send_time ) <= 3

UNION ALL
SELECT
    tc.id,
    es.email,
    es.update_time
FROM
    ods.vova_email_send_log_b es
    INNER JOIN ods.vova_email_task_config tc ON es.config_id = tc.id
WHERE
    es.OPEN = 1
    AND tc.app_name = 'florynight'
    AND datediff( es.update_time, tc.send_time ) <= 3

UNION ALL
SELECT
    tc.id,
    es.email,
    es.update_time
FROM
    ods.vova_email_send_log_c es
    INNER JOIN ods.vova_email_task_config tc ON es.config_id = tc.id
WHERE
    es.OPEN = 1
    AND tc.app_name = 'florynight'
    AND datediff( es.update_time, tc.send_time ) <= 1
    )
GROUP BY
    id,
    email
    ),
    tmp_pay AS (
SELECT
    email,
    oi.pay_time,
    og.goods_number * og.shop_price AS total_price
FROM
    ods.fn_trigram_shopping_order_info oi
    INNER JOIN ods.fn_trigram_shopping_order_goods og ON og.order_id = oi.order_id
    LEFT JOIN dwd.fn_dim_goods fdg ON fdg.goods_id = og.goods_id
    AND oi.pay_status >= 1
    AND fdg.datasource = 'florynight'
    )

insert overwrite table rpt.rpt_newsletter_monitor_task
SELECT
    tmp1.id,
    tc.app_name,
    tc.name task_name,
    tc.send_time,
    tc.success_num as succ_num,
    nvl(tmp1.gmv_1d,0) as gmv_1d,
    nvl ( tmp1.gmv_1d / tc.success_num*10000, 0 ) as rate
FROM
    (
SELECT
    tmp_news_email.id,
    sum( tmp_pay.total_price ) AS gmv_1d
FROM
    tmp_news_email
    LEFT JOIN tmp_pay ON tmp_news_email.email = tmp_pay.email
WHERE
    datediff( tmp_pay.pay_time, tmp_news_email.login_time ) < 1
GROUP BY
    tmp_news_email.id

UNION ALL
SELECT
    id,
    gmv_1d
FROM
    rpt.rpt_newsletter_callback_email_detail_v2
    ) tmp1
    INNER JOIN ods.vova_email_task_config tc ON tmp1.id = tc.id
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 15G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=rpt_newsletter_monitor_task" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

