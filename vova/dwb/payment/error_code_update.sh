#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

#TEST -- regexp_extract(txn_post,"('_responseCode'|'response_code') => '([0-9]+)'", 2) as error_code

sql="
insert overwrite table tmp.tmp_dwb_vova_payment_error_code_base
SELECT /*+ REPARTITION(5) */
       date(oi.order_time) as event_date,
       oi.order_sn,
       pt.payment_code,
       pt.txn_post
FROM ods_vova_vts.ods_vova_order_info oi
    inner join ods_vova_vts.ods_vova_paypal_txn pt on pt.order_sn = oi.order_sn
WHERE date(oi.order_time) <= '${cur_date}'
  AND date(oi.order_time) >= date_sub('${cur_date}', 7)
  AND oi.email NOT REGEXP '@tetx.com|@i9i8.com'
  AND oi.parent_order_id = 0
;

-- get除金率不足error_code
insert overwrite table tmp.tmp_dwb_vova_payment_error_code
select
/*+ REPARTITION(5) */
event_date,
order_sn,
error_code
from
(
select
event_date,
order_sn,
regexp_extract(txn_post, '(\'_responseCode\'|\'response_code\') => \'([0-9]+)\'', 2) as error_code
from
tmp.tmp_dwb_vova_payment_error_code_base
where payment_code = 'checkout'
) t1
where error_code != ''

UNION

select
event_date,
order_sn,
error_code
from
(
select
event_date,
order_sn,
regexp_extract(txn_post, '<ERROR><CODE>([0-9]+)</CODE>', 1) as error_code
from
tmp.tmp_dwb_vova_payment_error_code_base
where payment_code = 'gc_merchant_link'
) t1
where error_code != ''

UNION

select
event_date,
order_sn,
error_code
from
(
select
event_date,
order_sn,
regexp_extract(txn_post, '\'processorResponseCode\' => \'([0-9]+)\'', 1) as error_code
from
tmp.tmp_dwb_vova_payment_error_code_base
where payment_code = 'braintree_paypal'
) t1
where error_code != ''

UNION

select
event_date,
order_sn,
error_code
from
(
select
event_date,
order_sn,
regexp_extract(txn_post, '\"L_ERRORCODE0\":\"([0-9]+)\"', 1) as error_code
from
tmp.tmp_dwb_vova_payment_error_code_base
where payment_code = 'paypal'
) t1
where error_code != ''
;

"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_vova_payment_error_code_base" \
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