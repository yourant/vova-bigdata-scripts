#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dwd/dwd_vova_fact_act_ord_gs
sql="
insert overwrite table dwd.dwd_vova_fact_act_ord_gs
select /*+ REPARTITION(3) */
       byr.datasource                               as datasource,
       loi.activity_id                              as act_id,
       'luckystar'                                  as act_name,
       concat('luckystar_', loi.luckystar_order_id) as uiq_vtl_ord_id,
       loi.luckystar_order_id                       as vtl_ord_id,
       concat('luckystar_', 0)                      as uiq_vtl_ord_gs_id,
       0                                            as vtl_ord_gs_id,
       lgm.order_id                                 as ord_id,
       og.rec_id                                    as ord_gs_id,
       loi.user_id                                  as byr_id,
       loi.payment_id                               as pmt_id,
       loi.order_status                             as ord_sts,
       loi.pay_status                               as pay_sts,
       loi.pay_time                                 as pay_time,
       0.00                                         as ship_fee,
       loi.bonus                                    as bonus,
       loi.order_amount                             as ord_amt,
       loi.goods_number                             as gs_cnt,
       loi.goods_id                                 as gs_id,
       loi.sku_id                                   as sku_id,
       loi.create_time                              as ord_time,
       loi.receive_time                             as rcv_time,
       loi.sm_id                                    as sm_id,
       byr.region_code                              as rgn_code,
       'NA'                                         as dvc_id,
       byr.platform                                 as platform,
       byr.gender                                   as gender,
       loi.last_update_time                         as last_update_time
from ods_vova_vts.ods_vova_luckystar_order_info loi
         inner join ods_vova_vts.ods_vova_luckystar_group_member lgm on loi.luckystar_order_id = lgm.luckystar_order_id
         inner join dim.dim_vova_buyers byr on byr.buyer_id = loi.user_id
         left join ods_vova_vts.ods_vova_order_goods og on og.order_id = lgm.order_id
;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
#spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=40" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwd_vova_fact_act_ord_gs" --conf "spark.sql.autoBroadcastJoinThreshold=10485760" --conf "spark.sql.output.merge=true"   -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi