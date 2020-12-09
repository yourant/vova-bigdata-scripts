#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
insert overwrite table dwd.dwd_vova_fact_refund
select
      CASE
           WHEN oi.from_domain LIKE '%vova%' THEN 'vova'
           WHEN oi.from_domain LIKE '%airyclub%' THEN 'airyclub'
           ELSE 'NA'
           END                                                                   AS datasource,
       rr.refund_id,
       rr.order_goods_id,
       rr.refund_type_id,
       rr.risk_type,
       rr.refund_type,
       rat.refund_reason_type_id,
       rat.refund_reason,
       rr.display_currency_id,
       rr.order_currency_id,
       rr.refund_wallet,
       rr.refund_amount,
       rr.bonus,
       rr.refund_amount_exchange,
       rr.bonus_exchange,
       rr.display_refund_amount_exchange,
       rr.display_bonus_exchange,
       rr.create_time,
       rr.real_refund_amount_exchange,
       rr.exec_refund_time,
       rat.audit_status,
       rat.audit_time,
       ogs.sku_pay_status,
       rat.recheck_type
from ods_vova_themis.ods_vova_refund_reason rr
         left join (select *
                    FROM (select rrt.value as refund_reason,
                                 rat.order_goods_id,
                                 rrt.id as refund_reason_type_id,
                                 rat.audit_status,
                                 rat.audit_time,
                                 rat.recheck_type,
                                 row_number()
                                         over (partition by rat.order_goods_id order by rat.last_update_time desc) as rank
                          from ods_vova_themis.ods_vova_refund_audit_txn rat
                                   LEFT JOIN ods_vova_themis.ods_vova_refund_reason_type rrt ON rrt.id = rat.refund_reason_type_id) as rat
                    where rat.rank = 1
         ) rat on rat.order_goods_id = rr.order_goods_id
left join ods_vova_themis.ods_vova_order_goods_status ogs on ogs.order_goods_id = rr.order_goods_id
left join ods_vova_themis.ods_vova_order_goods og on og.rec_id = rr.order_goods_id
LEFT JOIN ods_vova_themis.ods_vova_order_info oi ON oi.order_id = og.order_id
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.app.name=dwd_vova_fact_refund" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


