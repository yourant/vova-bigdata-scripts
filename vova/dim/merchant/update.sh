#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  cur_date=$(date -d "-1 day" +%Y-%m-%d)
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dim/dim_vova_merchant
###逻辑sql
sql="
insert overwrite table dim.dim_vova_merchant
select /*+ REPARTITION(1) */ 'vova' as datasource,
       m.merchant_id AS mct_id,
       m.create_time AS reg_time,
       m.merchant_sn AS mct_sn,
       m.store_name AS mct_name,
       m.store_name_cn AS mct_name_cn,
       m.store_category AS mct_cat_desc,
       m.sale_country AS sale_region_desc,
       m.address,
       m.account_type,
       m.logistics_type AS logistics_type_desc,
       m.register_email AS reg_email,
       m.email,
       m.phone,
       m.weixin AS we_chat,
       m.qq,
       m.is_delete,
       m.is_banned,
       m.is_on_vacation,
       m.review_status,
       case when  m.is_delete=1  then 1
            when  m.is_on_vacation=1 then 2
            when  m.is_delete=0 and m.review_status='passed' then 3
            when  m.is_banned=1 then 4
           else ' '
       end  mct_status,
       t1.first_publish_time,
       CASE
           WHEN DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), t1.first_publish_time) <= 28 THEN 'tag1'
           WHEN DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), t1.first_publish_time) > 28
                AND DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), t1.first_publish_time) <= 56 THEN 'tag2'
           WHEN DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), t1.first_publish_time) > 56
                AND DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), t1.first_publish_time) <= 84 THEN 'tag3'
           END tag,
       order_temp.first_customer_buy_time,
       m.settle_status,
       vmd.legal_person_card_number as card_nbr,
       vmd.equipment_ip as reg_ip,
       t2.bank_nbr as bank_nbr,
       t3.pay_email as paypal,
       t4.eqmnt,
       vmd.legal_person_name slr_name,
       vmd.legal_person_address slr_addr,
       t5.used_time coupon_time,
       t3.pay_time deposit_time,
       t6.nick spsor_name,
       CASE
         WHEN (t5.coupon_code <> '' AND t5.coupon_code IS NOT NULL) THEN t5.used_time
         WHEN t3.pay_time IS NOT NULL THEN t3.pay_time
         ELSE NULL
         END AS pay_or_verify_time
FROM ods_vova_vts.ods_vova_merchant m
LEFT JOIN
  (select g.merchant_id,
          min(gosr.create_time) AS first_publish_time
   FROM ods_vova_vts.ods_vova_goods g
   LEFT JOIN ods_vova_vts.ods_vova_goods_on_sale_record gosr ON gosr.goods_id = g.goods_id and gosr.action='on'
   GROUP BY g.merchant_id) t1 ON m.merchant_id = t1.merchant_id
LEFT JOIN (SELECT g.merchant_id, min(oi.pay_time) AS first_customer_buy_time
FROM ods_vova_vts.ods_vova_order_info oi
         INNER JOIN ods_vova_vts.ods_vova_order_goods og ON oi.order_id = og.order_id
         INNER JOIN ods_vova_vts.ods_vova_goods g ON og.goods_id = g.goods_id
WHERE oi.pay_status >= 1
  AND oi.parent_order_id = 0
  AND oi.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
GROUP BY g.merchant_id) order_temp ON m.merchant_id = order_temp.merchant_id
----取出身份证号、注册ip
left outer join
ods_vova_vts.ods_vova_merchant_detail vmd
on m.merchant_id=vmd.merchant_id
left outer join
--取银行卡号
(
select merchant_id,collect_set(receiver_bank_account) bank_nbr
from ods_vova_vts.ods_vova_merchant_payment_account_info group by merchant_id
) t2
on m.merchant_id=t2.merchant_id
left outer join
--取paypal,缴纳押金时间
(
select  merchant_id,min(pay_time) pay_time,collect_set(pay_email) pay_email from ods_vova_vts.ods_vova_merchant_register_deposit_payment group by merchant_id
) t3
on m.merchant_id=t3.merchant_id
left outer join
--取设备号
(
select  merchant_id,collect_set(equipment) as eqmnt from ods_vova_vts.ods_vova_merchant_login_log group by merchant_id
) t4
on m.merchant_id=t4.merchant_id
--填写邀请码时间
left outer join
(
select  used_merchant_id merchant_id,used_time,coupon_code from  ods_vova_vts.ods_vova_merchant_register_coupon where coupon_code is not null and used_time > '1970-01-01 00:00:01'
) t5
on m.merchant_id=t5.merchant_id
--招商人姓名
left join
(
SELECT  ms.merchant_id, s.nick from
(select merchant_id,max(merchant_sponsor_id) merchant_sponsor_id from ods_vova_vts.ods_vova_merchant_sponsor group by merchant_id) temp
inner join ods_vova_vts.ods_vova_merchant_sponsor ms on ms.merchant_sponsor_id = temp.merchant_sponsor_id and ms.merchant_id = temp.merchant_id
JOIN ods_vova_vts.ods_vova_sponsor s ON s.sponsor_id = ms.sponsor_id
) t6
on m.merchant_id=t6.merchant_id
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.app.name=dim_vova_merchant"  --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
