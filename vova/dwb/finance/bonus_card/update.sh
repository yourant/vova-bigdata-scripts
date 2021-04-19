#!/bin/bash
#指定日期和引擎
last_month=$1
#默认日期为昨天
if [ ! -n "$1" ];then
last_month=`date -d "-1 month" +%Y-%m-01`
fi
echo "$last_month"
sql="

drop table if exists tmp.tmp_dwb_vova_finance_bonus_card_base;
create table tmp.tmp_dwb_vova_finance_bonus_card_base as
SELECT
bc.id
from
ods_vova_vts.ods_vova_bonus_card bc
WHERE bc.status = 'paid'
  AND (
        trunc(from_unixtime(bc.start_time, 'yyyy-MM-dd'), 'MM') = '${last_month}'
        OR
        trunc(from_unixtime(bc.end_time, 'yyyy-MM-dd'), 'MM') = '${last_month}'
    )
;

INSERT OVERWRITE TABLE dwb.dwb_vova_finance_bonus_card PARTITION (pt = '${last_month}')
select
'vova' AS datasource,
concat(trunc('${last_month}','MM'),'---', last_day('${last_month}')) AS interval_date,
case
when from_unixtime(bc.end_time, 'yyyy-MM-dd') <= '${last_month}' then '【已完结】'
when from_unixtime(bc.end_time, 'yyyy-MM-dd') > '${last_month}' then '【生效中】'
else '' end as life_cycle,
bc.id AS bonus_card_id,
bc.user_id,
bc.price,
from_unixtime(bc.start_time, 'yyyy-MM-dd HH:mm:ss') AS bonus_start,
from_unixtime(bc.end_time, 'yyyy-MM-dd HH:mm:ss') AS bonus_end,
bonus_interval_issue.currency,
month_issue.issue_amount,
month_use.bonus,
month_issue.valid_amount,
month_use.order_cnt,
month_use.order_amount,
bonus_interval_issue.issue_amount AS issue_amount_interval,
bonus_interval_use.bonus AS bonus_interval,
bonus_interval_issue.valid_amount AS valid_amount_interval,
bonus_interval_use.order_cnt AS order_cnt_interval,
bonus_interval_use.order_amount AS order_amount_interval,
if(
last_day('${last_month}') >= from_unixtime(bc.end_time, 'yyyy-MM-dd'),
(datediff(from_unixtime(bc.end_time, 'yyyy-MM-dd'),trunc('2021-03-01','MM')) + 1) * bc.price / 31,
(datediff(last_day('2021-03-01'),from_unixtime(bc.start_time, 'yyyy-MM-dd')) + 1) * bc.price / 31
) AS income

from
ods_vova_vts.ods_vova_bonus_card bc
INNER JOIN tmp.tmp_dwb_vova_finance_bonus_card_base bcb on bcb.id = bc.id
LEFT JOIN (
SELECT
bc.id,
dc.currency,
sum(dc.cpn_cfg_val) AS issue_amount,
sum(if( from_unixtime(unix_timestamp(dc.cpn_create_time) + if(oc.extend_day>0, oc.extend_day * 3600 * 24, oc.extend_length), 'yyyy-MM-dd') > last_day('${last_month}'),dc.cpn_cfg_val, 0)) AS valid_amount
FROM
dim.dim_vova_coupon dc
INNER JOIN ods_vova_vts.ods_vova_bonus_card bc on bc.user_id = dc.buyer_id
INNER JOIN tmp.tmp_dwb_vova_finance_bonus_card_base bcb on bcb.id = bc.id
LEFT JOIN ods_vova_vts.ods_vova_ok_coupon oc ON oc.coupon_id = dc.cpn_id
WHERE dc.cpn_cfg_type_id in
('470', '469', '468', '467', '466', '465', '464', '463', '461',
'460', '459', '458', '457', '456', '455', '454', '453', '452',
'451', '450', '449', '448', '447', '446', '445', '444', '443',
'442', '441', '439')
AND trunc(dc.cpn_create_time, 'MM') = '${last_month}'
AND unix_timestamp(dc.cpn_create_time) >= bc.start_time
AND unix_timestamp(dc.cpn_create_time) <= bc.end_time
group by bc.id, dc.currency
) month_issue on month_issue.id = bc.id
LEFT JOIN (
SELECT
bc.id,
- sum(oi.bonus) AS bonus,
count(DISTINCT oi.order_id) AS order_cnt,
sum(oi.order_amount) AS order_amount
FROM
dim.dim_vova_coupon dc
INNER JOIN ods_vova_vts.ods_vova_bonus_card bc on bc.user_id = dc.buyer_id
INNER JOIN tmp.tmp_dwb_vova_finance_bonus_card_base bcb on bcb.id = bc.id
INNER JOIN ods_vova_vts.ods_vova_order_info oi on oi.coupon_code = dc.cpn_code
WHERE dc.cpn_cfg_type_id in
('470', '469', '468', '467', '466', '465', '464', '463', '461',
'460', '459', '458', '457', '456', '455', '454', '453', '452',
'451', '450', '449', '448', '447', '446', '445', '444', '443',
'442', '441', '439')
AND trunc(oi.pay_time, 'MM') = '${last_month}'
AND unix_timestamp(dc.cpn_create_time) >= bc.start_time
AND unix_timestamp(dc.cpn_create_time) <= bc.end_time
AND oi.pay_status >= 1
AND oi.parent_order_id = 0
group by bc.id
) month_use on month_use.id = bc.id
LEFT JOIN (
SELECT
bc.id,
dc.currency,
sum(dc.cpn_cfg_val) AS issue_amount,
sum(if( from_unixtime(unix_timestamp(dc.cpn_create_time) + if(oc.extend_day>0, oc.extend_day * 3600 * 24, oc.extend_length), 'yyyy-MM-dd') > last_day('${last_month}'),dc.cpn_cfg_val, 0)) AS valid_amount
FROM
dim.dim_vova_coupon dc
INNER JOIN ods_vova_vts.ods_vova_bonus_card bc on bc.user_id = dc.buyer_id
INNER JOIN tmp.tmp_dwb_vova_finance_bonus_card_base bcb on bcb.id = bc.id
LEFT JOIN ods_vova_vts.ods_vova_ok_coupon oc ON oc.coupon_id = dc.cpn_id
WHERE dc.cpn_cfg_type_id in
('470', '469', '468', '467', '466', '465', '464', '463', '461',
'460', '459', '458', '457', '456', '455', '454', '453', '452',
'451', '450', '449', '448', '447', '446', '445', '444', '443',
'442', '441', '439')
AND unix_timestamp(dc.cpn_create_time) >= bc.start_time
AND unix_timestamp(dc.cpn_create_time) <= bc.end_time
AND trunc(dc.cpn_create_time, 'MM') <= '${last_month}'
group by bc.id, dc.currency
) bonus_interval_issue on bonus_interval_issue.id = bc.id
LEFT JOIN (
SELECT
bc.id,
- sum(oi.bonus) AS bonus,
count(DISTINCT oi.order_id) AS order_cnt,
sum(oi.order_amount) AS order_amount
FROM
dim.dim_vova_coupon dc
INNER JOIN ods_vova_vts.ods_vova_bonus_card bc on bc.user_id = dc.buyer_id
INNER JOIN tmp.tmp_dwb_vova_finance_bonus_card_base bcb on bcb.id = bc.id
INNER JOIN ods_vova_vts.ods_vova_order_info oi on oi.coupon_code = dc.cpn_code
WHERE dc.cpn_cfg_type_id in
('470', '469', '468', '467', '466', '465', '464', '463', '461',
'460', '459', '458', '457', '456', '455', '454', '453', '452',
'451', '450', '449', '448', '447', '446', '445', '444', '443',
'442', '441', '439')
AND unix_timestamp(dc.cpn_create_time) >= bc.start_time
AND unix_timestamp(dc.cpn_create_time) <= bc.end_time
AND trunc(oi.pay_time, 'MM') <= '${last_month}'
AND oi.pay_status >= 1
AND oi.parent_order_id = 0
group by bc.id
) bonus_interval_use on bonus_interval_use.id = bc.id
;

"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.app.name=dwb_vova_finance_bonus_card" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi