#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

job_name="dwb_vova_high_refund_goods_gsn_req8748_chenkai_${cur_date}"

###逻辑sql
sql="
insert overwrite table dwb.dwb_vova_high_refund_goods_gsn partition(pt='${cur_date}')
select
  goods_sn_new,
  dg.first_cat_name first_cat_name,
  nvl(sum(shipping_fee+shop_price*goods_number), 0) gmv_last_day7
from
(
  select
    distinct goods_sn_new goods_sn_new
  from
  (
    select
      dgg.pt pt,
      dg.goods_sn goods_sn_new, -- 变更后的 goods_sn
      count(distinct(if(dg.is_on_sale = 1, dg.goods_id, null))) on_sale_goods_cnt -- 在架商品数
    from
      dwb.dwb_vova_goods_gsn dgg
    left join
      dim.dim_vova_goods dg
    on dgg.goods_id = dg.goods_id
    where dgg.goods_sn like 'SN%' and dg.goods_sn like 'GSN%'
      and dgg.pt= date_sub('${cur_date}', 1)  -- 由于屏蔽数据放在 t-1 分区，所以昨天屏蔽的商品在前天分区
    group by dgg.pt, dg.goods_sn

    UNION

    select
      dgg.pt pt,
      dg.goods_sn goods_sn_new, -- 变更后的 goods_sn
      count(distinct(if(dg.is_on_sale = 1, dg.goods_id, null))) on_sale_goods_cnt -- 在架商品数
    from
      dwb.dwb_vova_goods_gsn dgg
    left join
      dim.dim_vova_goods dg
    on dgg.goods_id = dg.goods_id
    where dg.goods_sn like 'GSN%'
      and dgg.pt= '${cur_date}'
    group by dgg.pt, dg.goods_sn
  )
  where on_sale_goods_cnt = 0
) t1
left join
(
  select distinct
    goods_sn goods_sn,
    first_cat_name first_cat_name
  from
    dim.dim_vova_goods
) dg
on t1.goods_sn_new = dg.goods_sn
left join
(
  select
  *
  from
    dwd.dwd_vova_fact_pay
  where to_date(pay_time) <= '${cur_date}' and to_date(pay_time) >= date_sub('${cur_date}', 7)
) fp
on t1.goods_sn_new = fp.goods_sn
group by t1.goods_sn_new, dg.first_cat_name
;

"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 10G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
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
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`


spark-submit \
--deploy-mode client \
--name 'vova_gsn_goods_send' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "select goods_sn,first_cat_name,gmv_last_day7 from dwb.dwb_vova_high_refund_goods_gsn where pt='${cur_date}'"  \
-head "goods_sn,一级品类名称,近7日gmv"  \
-receiver "kai.cheng@vova.com.hk,Fusang@vova.com.hk,Fiona.yang@vova.com.hk,lvyao@vova.com.hk" \
-title "屏蔽sn商品转gsn未被跟卖商品统计" \
--type attachment \
--fileName "屏蔽sn商品转gsn未被跟卖商品统计_${cur_date}"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi











