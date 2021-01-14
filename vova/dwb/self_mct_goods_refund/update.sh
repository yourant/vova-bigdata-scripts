#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`


#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

start_date='2020-04-01'

echo "start_date: ${start_date}"
echo "cur_date: ${cur_date}"
job_name="dwb_vova_sel_mct_goods_refund_req5027_chenkai_${cur_date}"

###逻辑sql
sql="
insert OVERWRITE TABLE dwb.dwb_vova_sel_mct_goods_refund
select
/*+ REPARTITION(1) */
tmp_re.datasource,
tmp_re.virtual_goods_id,
dg.goods_sn,
tmp_re.goods_number,
tmp_re.goods_number_last7,
tmp_re.gmv,
tmp_re.refund_order_goods_cnt,
tmp_re.confirm_order_goods_cnt_before45,
tmp_re.refund_order_goods_cnt_within45,
tmp_re.confirm_order_goods_cnt_before90,
tmp_re.refund_order_goods_cnt_within90,
tmp_re.confirm_order_goods_cnt,
tmp_re.defective_refund_order_goods_cnt,
tmp_re.doesnt_fit_refund_order_goods_cnt,
tmp_re.not_as_described_refund_order_goods_cnt,
tmp_re.not_receive_yet_refund_order_goods_cnt,
tmp_re.others_refund_order_goods_cnt,
tmp_re.poor_quality_refund_order_goods_cnt,
tmp_re.wrong_product_refund_order_goods_cnt,
tmp_re.wrong_quantity_refund_order_goods_cnt,
'${cur_date}'
from
 (select
  tmp_dog.datasource datasource,
  tmp_dog.virtual_goods_id virtual_goods_id,
  sum(if(tmp_dog.sku_pay_status=2,tmp_dog.goods_number, 0)) goods_number,
  sum(if(tmp_dog.sku_pay_status=2 and datediff('${cur_date}', tmp_dog.pay_time) <= 7, tmp_dog.goods_number,0  )) goods_number_last7,
  sum(if(tmp_dog.sku_pay_status=2, tmp_dog.shop_price*tmp_dog.goods_number+tmp_dog.shipping_fee, 0)) gmv,
  sum(if(tmp_dog.sku_pay_status=4,1,0)) refund_order_goods_cnt,
  sum(if(datediff('${cur_date}', tmp_dog.confirm_time) >= 45, 1, 0)) confirm_order_goods_cnt_before45,
  sum(if(datediff('${cur_date}', tmp_dog.confirm_time) >= 45 and to_date(fr.exec_refund_time) >= '${start_date}' and datediff(fr.exec_refund_time, tmp_dog.confirm_time )  <= 45, 1, 0)) refund_order_goods_cnt_within45,
  sum(if(datediff('${cur_date}', tmp_dog.confirm_time) >= 90, 1, 0)) confirm_order_goods_cnt_before90,
  sum(if(datediff('${cur_date}', tmp_dog.confirm_time) >= 90 and to_date(fr.exec_refund_time) >= '${start_date}' and datediff(fr.exec_refund_time, tmp_dog.confirm_time )  <= 90, 1, 0)) refund_order_goods_cnt_within90,
  count(distinct(tmp_dog.order_goods_id)) confirm_order_goods_cnt,
  sum(if(fr.refund_reason_type_id=4, 1, 0)) defective_refund_order_goods_cnt,
  sum(if(fr.refund_reason_type_id=1, 1, 0)) doesnt_fit_refund_order_goods_cnt,
  sum(if(fr.refund_reason_type_id=3, 1, 0)) not_as_described_refund_order_goods_cnt,
  sum(if(fr.refund_reason_type_id=8, 1, 0)) not_receive_yet_refund_order_goods_cnt,
  sum(if(fr.refund_reason_type_id=9, 1, 0)) others_refund_order_goods_cnt,
  sum(if(fr.refund_reason_type_id=2, 1, 0)) poor_quality_refund_order_goods_cnt,
  sum(if(fr.refund_reason_type_id=6, 1, 0)) wrong_product_refund_order_goods_cnt,
  sum(if(fr.refund_reason_type_id=7, 1, 0)) wrong_quantity_refund_order_goods_cnt
 from
 (select  *
  from dim.dim_vova_order_goods dog
  where dog.sku_order_status >= 1 and to_date(confirm_time) >= '${start_date}' and mct_id in (26414, 11630, 36655)) tmp_dog
  left join
  dwd.dwd_vova_fact_refund fr
  on tmp_dog.datasource = fr.datasource and tmp_dog.order_goods_id = fr.order_goods_id
  group by
  tmp_dog.datasource,tmp_dog.virtual_goods_id
 ) tmp_re
 left join
 dim.dim_vova_goods dg
 on tmp_re.virtual_goods_id = dg.virtual_goods_id
 where tmp_re.goods_number is not null and tmp_re.goods_number > 0
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 5G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=100" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

echo "end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
