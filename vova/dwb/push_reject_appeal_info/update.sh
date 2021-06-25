#!/bin/bash
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-sql \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=mlb_vova_user_behavior_link_before" \
-e "
INSERT OVERWRITE TABLE dwb.dwb_vova_push_reject_appeal_info partition (pt = '${cur_date}')
select '${cur_date}',
       a.mct_id,
       count(distinct c.order_goods_id)                                               mct_reject_order_cnt,
       count(distinct a.order_goods_id)                                              send_order_cnt,
       round(count(distinct c.order_goods_id) / count(distinct a.order_goods_id), 2) mct_reject_send_rate
from dim.dim_vova_order_goods a
         left join dim.dim_vova_merchant b on a.mct_id = b.mct_id
         left join (SELECT rr.order_goods_id
                    FROM ods_vova_vts.ods_vova_refund_reason rr
                             join ods_vova_vts.ods_vova_refund_audit_txn rat1 on rr.order_goods_id = rat1.order_goods_id
                             join ods_vova_vts.ods_vova_refund_audit_txn rat2 on rr.order_goods_id = rat2.order_goods_id
                    where rat1.audit_status = 'mct_audit_rejected'
                      and rat2.recheck_type = 2
                    group by rr.order_goods_id) c on a.order_goods_id = c.order_goods_id
where to_date(a.shipping_time) >= date_sub('${cur_date}', 88)
  and to_date(a.shipping_time) <= date_sub('${cur_date}', 58)
  and sku_shipping_status >= 1
  and a.datasource = 'vova'
group by a.mct_id,
         b.mct_name
;"


spark-submit \
--deploy-mode client \
--master yarn  \
--num-executors 3 \
--executor-cores 1 \
--executor-memory 8G \
--driver-memory 8G \
--conf spark.app.name=vova_push_reject_appeal_info \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.eventLog.enabled=false \
--driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--class com.vova.process.SendData2Interface s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
--sql "select cur_date,merchant_id,reject_appeal_num,shipping_num,reject_appeal_rate from dwb.dwb_vova_push_reject_appeal_info where pt = '${cur_date}'" \
--url " https://merchant.vova.com.hk/api/v1/internal/Order/pushRejectAppealInfo" \
--secretKey  "aik2Oo2he5Aiaa2hahpaPoodha2b" \
--batchSize 100 \
--id vova_push_reject_appeal_info

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
