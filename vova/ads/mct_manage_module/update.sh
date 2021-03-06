#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="ads_vova_mct_manage_module_req7747_chenkai_${cur_date}"

###逻辑sql
sql="
create table if not exists tmp.tmp_no_brand_avg_cr_last7d_${table_suffix} as
select /*+ REPARTITION(1) */
  tmp1.mct_id mct_id,
  tmp1.first_cat_id first_cat_id,
  sum(nvl(no_brand_pay_uv / no_brand_impression_uv, 0)) / 7 no_brand_avg_cr_last7d
from
(
  select
    flgi.pt pt,
    dg.mct_id mct_id,
    dg.first_cat_id first_cat_id,
    count(distinct device_id) no_brand_impression_uv
  from
    dwd.dwd_vova_log_goods_impression flgi
  left join
    dim.dim_vova_goods dg
  on flgi.datasource = dg.datasource and flgi.virtual_goods_id = dg.virtual_goods_id
  where flgi.datasource = 'vova'
    and flgi.pt <= '${cur_date}' and flgi.pt > date_sub('${cur_date}', 7)
    and dg.brand_id = 0
  group by flgi.pt, dg.mct_id, dg.first_cat_id
) tmp1
left join
(
  select
    to_date(fp.pay_time) pt,
    dg.mct_id mct_id,
    dg.first_cat_id first_cat_id,
    count(distinct fp.device_id) no_brand_pay_uv
  from
    dwd.dwd_vova_fact_pay fp
  left join
    dim.dim_vova_goods dg
  on fp.datasource = dg.datasource and fp.goods_id = dg.goods_id
  where fp.datasource = 'vova'
    and to_date(fp.pay_time) <= '${cur_date}' and to_date(fp.pay_time) > date_sub('${cur_date}', 7)
    and dg.brand_id = 0
  group by to_date(fp.pay_time), dg.mct_id, dg.first_cat_id
) tmp2
on tmp1.pt = tmp2.pt and tmp1.mct_id = tmp2.mct_id and tmp1.first_cat_id = tmp2.first_cat_id
group by tmp1.mct_id, tmp1.first_cat_id
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
--conf "spark.default.parallelism=300" \
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

sql="
insert overwrite table ads.ads_vova_mct_manage_module PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
  t1.mct_id                        mct_id,
  dm.mct_name                      mct_name,
  t1.first_cat_id                  first_cat_id,
  dg.first_cat_name                first_cat_name,
  nvl(amr.rank, 0)                 rank,
  dm.mct_status                    mct_status,
  nvl(t1.gmv_day7                  , 0) gmv_day7,
  nvl(t1.sn_goods_count            , 0) sn_goods_count,
  nvl(t1.sn_new_goods_count        , 0) sn_new_goods_count,
  nvl(t1.gsn_goods_count           , 0) gsn_goods_count,
  nvl(t1.new_gsn_goods_count       , 0) new_gsn_goods_count,
  nvl(t1.sold_goods_count          , 0) sold_goods_count,
  nvl(t1.on_sale_goods_count       , 0) on_sale_goods_count,
  nvl(t1.sold_new_goods_count      , 0) sold_new_goods_count,
  nvl(t1.on_sale_new_goods_count   , 0) on_sale_new_goods_count,
  nvl(t1.on_sale_brand_goods_count , 0) on_sale_brand_goods_count,
  nvl(t2.lrf_order_goods_count_9_12, 0) lrf_order_goods_count_9_12,
  nvl(t2.tot_order_goods_count_9_12, 0) tot_order_goods_count_9_12,
  nvl(t3.nlrf_order_goods_count_5_8, 0) nlrf_order_goods_count_5_8,
  nvl(t3.tot_order_goods_count_5_8, 0)  tot_order_goods_count_5_8,
  nvl(round(sold_goods_count / on_sale_goods_count, 4), 0)          goods_turnover_rate,
  nvl(round(sold_new_goods_count / on_sale_new_goods_count, 4), 0)  new_goods_turnover_rate,
  nvl(round(on_sale_brand_goods_count / on_sale_goods_count, 4), 0) on_sale_brand_goods_count_rate,
  nvl(round(t2.lrf_rate_9_12w, 4), 0) lrf_rate_9_12w,
  nvl(round(t3.nlrf_rate_5_8w, 4), 0) nlrf_rate_5_8w,
  nvl(no_brand_gmv_last7d, 0) no_brand_gmv_last7d,
  nvl(no_brand_gmv_last1d, 0) no_brand_gmv_last1d,
  round(nvl(tmp.no_brand_avg_cr_last7d, 0), 4) no_brand_avg_cr_last7d
from
(
  select
    mct_id        mct_id,
    first_cat_id  first_cat_id,
    sum(gmv_day7) gmv_day7, -- 近7日gmv
    sum(no_brand_gmv_last7d) no_brand_gmv_last7d, -- 近7日非brand gmv
    sum(no_brand_gmv_last1d) no_brand_gmv_last1d, -- 近1日非brand gmv
    count(distinct sn_goods_id)            sn_goods_count, -- 在架sn商品数
    count(distinct sn_new_goods_id)        sn_new_goods_count, -- 近一月在架上新sn商品数
    count(distinct gsn_goods_id)           gsn_goods_count, -- 在架克隆gsn商品量
    count(distinct new_gsn_goods_id)       new_gsn_goods_count, -- 近一月在架上新克隆gsn商品数
    count(distinct sold_goods_id)          sold_goods_count, -- 出单商品数
    count(distinct on_sale_goods_id)       on_sale_goods_count, -- 总在架商品数
    count(distinct sold_new_goods_id)      sold_new_goods_count, -- 近一月上新商品中出单商品数
    count(distinct on_sale_new_goods_id)   on_sale_new_goods_count, -- 近1月在架上新商品数
    count(distinct on_sale_brand_goods_id) on_sale_brand_goods_count -- 在架brand商品数
  from
  (
    select
      dg.mct_id,
      dg.first_cat_id,
      dg.goods_id,
      if(to_date(fp.pay_time) <= '${cur_date}' and to_date(fp.pay_time) > date_sub('${cur_date}', 7), fp.shop_price * fp.goods_number + fp.shipping_fee, 0) gmv_day7, -- 近7日gmv
      if(to_date(fp.pay_time) <= '${cur_date}' and to_date(fp.pay_time) > date_sub('${cur_date}', 7) and dg.brand_id = 0, fp.shop_price * fp.goods_number + fp.shipping_fee, 0) no_brand_gmv_last7d, -- 近7日非brand gmv
      if(to_date(fp.pay_time) = '${cur_date}' and dg.brand_id = 0, fp.shop_price * fp.goods_number + fp.shipping_fee, 0) no_brand_gmv_last1d, -- 近1日非brand gmv
      if(dg.goods_sn like 'SN%' and dg.is_on_sale = 1, dg.goods_id, null) sn_goods_id, -- 在架sn商品数
      if(dg.goods_sn like 'SN%' and dg.is_on_sale = 1 and to_date(dg.add_time) <= '${cur_date}' and to_date(dg.add_time) >= date_sub('${cur_date}', 30) , dg.goods_id, null) sn_new_goods_id, -- 近一月在架上新sn商品数
      if(dg.goods_sn like 'GSN%' and dg.is_on_sale = 1, dg.goods_id, null) gsn_goods_id, -- 在架克隆gsn商品数量
      if(dg.goods_sn like 'GSN%' and dg.is_on_sale = 1 and to_date(dg.add_time) <= '${cur_date}' and to_date(dg.add_time) >= date_sub('${cur_date}', 30) , dg.goods_id, null) new_gsn_goods_id, -- 近一月在架上新克隆gsn商品数
      if(to_date(fp.pay_time)='${cur_date}' and fp.order_goods_id is not null, fp.goods_id, null) sold_goods_id, -- 出单商品数
      if(dg.is_on_sale = 1, dg.goods_id, null) on_sale_goods_id, -- 总在架商品数
      if(to_date(dg.add_time) <= '${cur_date}' and to_date(dg.add_time) >= date_sub('${cur_date}', 30) and to_date(fp.pay_time)='${cur_date}' and fp.order_goods_id is not null, fp.goods_id, null) sold_new_goods_id, -- 近一月上新商品中出单商品数
      if(dg.is_on_sale = 1 and to_date(dg.add_time) <= '${cur_date}' and to_date(dg.add_time) >= date_sub('${cur_date}', 30), dg.goods_id, null) on_sale_new_goods_id, -- 近1月在架上新商品数
      if(dg.is_on_sale = 1 and dg.brand_id > 0, dg.goods_id, null) on_sale_brand_goods_id -- 在架brand商品数
    from
      dim.dim_vova_goods dg
    left join
      dwd.dwd_vova_fact_pay fp
    on dg.goods_id = fp.goods_id and dg.datasource = fp.datasource
    where dg.datasource = 'vova'
  )
  group by mct_id, first_cat_id
) t1
left join
  tmp.tmp_no_brand_avg_cr_last7d_${table_suffix} tmp
on t1.mct_id = tmp.mct_id and t1.first_cat_id = tmp.first_cat_id
left join
  dim.dim_vova_merchant dm
on t1.mct_id = dm.mct_id
left join
(
  select
    distinct first_cat_id, first_cat_name
  from
    dim.dim_vova_goods
) dg
on t1.first_cat_id = dg.first_cat_id
left join
(
  select
    *
  from
    ads.ads_vova_mct_rank
  where pt = '${cur_date}'
) amr
on t1.mct_id = amr.mct_id and t1.first_cat_id = amr.first_cat_id
left join
--9到12周物流退款率
(
  select
    t1.mct_id,
    t1.first_cat_id,
    sum(t1.lrf_order_cnt_9_12w) lrf_order_goods_count_9_12,
    count(t1.order_goods_id)    tot_order_goods_count_9_12,
    sum(t1.lrf_order_cnt_9_12w) / count(t1.order_goods_id) as lrf_rate_9_12w
  from
  (
    select og.mct_id,
      b.first_cat_id,
      og.order_goods_id,
      case
          when fr.refund_reason_type_id = 8 and fr.refund_type_id = 2 then 1
          else 0 end lrf_order_cnt_9_12w
    from dim.dim_vova_order_goods og
      join dim.dim_vova_goods b
        on og.goods_id = b.goods_id
      left join dwd.dwd_vova_fact_refund fr
        on fr.order_goods_id = og.order_goods_id
      left join dwd.dwd_vova_fact_logistics fl
        on fr.order_goods_id = fl.order_goods_id
    where datediff('${cur_date}', date(og.confirm_time)) between 63 and 84
      and og.sku_pay_status > 1
      and og.sku_shipping_status > 0
  ) t1
  group by t1.mct_id, t1.first_cat_id
) t2
on t1.mct_id = t2.mct_id and t1.first_cat_id = t2.first_cat_id
left join
-- 5到8周非物流退款率
(
  select
    t1.mct_id,
    t1.first_cat_id,
    sum(t1.nlrf_order_cnt_5_8w) nlrf_order_goods_count_5_8,
    count(t1.order_goods_id)    tot_order_goods_count_5_8,
    sum(t1.nlrf_order_cnt_5_8w) / count(t1.order_goods_id) as nlrf_rate_5_8w
  from
  (
    select og.mct_id,
      b.first_cat_id,
      og.order_goods_id,
      case
        when fr.refund_reason_type_id != 8 and fr.refund_type_id = 2 then 1
        else 0 end nlrf_order_cnt_5_8w
    from dim.dim_vova_order_goods og
    join dim.dim_vova_goods b
      on og.goods_id = b.goods_id
    left join dwd.dwd_vova_fact_refund fr
      on fr.order_goods_id = og.order_goods_id
    left join dwd.dwd_vova_fact_logistics fl
      on fr.order_goods_id = fl.order_goods_id
    where datediff('${cur_date}', date(og.confirm_time)) between 35 and 56
      and og.sku_pay_status > 1
      and og.sku_shipping_status > 0
  ) t1
  group by t1.mct_id, t1.first_cat_id
) t3
on t1.mct_id = t3.mct_id and t1.first_cat_id = t3.first_cat_id
where dm.mct_status in (2,3,4)
;

drop table if exists tmp.tmp_no_brand_avg_cr_last7d_${table_suffix};
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 10G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism=300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
