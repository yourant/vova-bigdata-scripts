#!/bin/bash
#指定日期和引擎
cur_date=$1
pre_month=$2
pre_2_month=$3
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_month=`date -d "1 month ago ${cur_date}" +%Y-%m-%d`
echo "pre_month=${pre_month}"
pre_2_month=`date -d "2 month ago ${cur_date}" +%Y-%m-%d`
echo "pre_2_month=${pre_2_month}"

###逻辑sql
sql="
INSERT OVERWRITE TABLE dwb.dwb_vova_mct_rank_data PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */ '${cur_date}' cur_date,
       regexp_replace(a.first_cat_name,'\'',''),
       a.rank,
       nvl(b.min_gmv,0),
       nvl(b.max_gmv,0),
       nvl(b.avg_gmv,0),
       nvl(a.expre_uv,0),
       nvl(f.pay_uv,0),
       nvl(f.avg_avg_person_price,0),
       nvl(c.avg_rep_rate_1mth,0),
       nvl(d.avg_nlrf_rate_5_8w,0),
       nvl(e.avg_lrf_rate_9_12w,0),
       nvl(a.expre_uv_cnt,0)

from (
        select nvl(tmp.first_cat_name, 'all') first_cat_name,
           nvl(tmp.rank, 'all')           rank,
           sum(expre_uv) expre_uv_cnt,
           floor(sum(expre_uv) /  count(distinct mct_id))                 expre_uv
    from (
             select b.first_cat_name,
                    c.rank,
                    b.mct_id,
                    count(distinct device_id) expre_uv

             from dwd.dwd_vova_log_goods_impression a
                      join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
                      join ads.ads_vova_mct_rank c on c.mct_id = b.mct_id and c.first_cat_id = b.first_cat_id
             where to_date(a.pt) = '${cur_date}'
               and c.pt = '${cur_date}'
             group by b.first_cat_name, c.rank, b.mct_id
         ) tmp
    group by cube (tmp.first_cat_name, tmp.rank)
     ) a
         left join (
        select nvl(tmp.first_cat_name, 'all')       first_cat_name,
                nvl(tmp.rank, 'all')                 rank,
                min(tmp.gmv)                         min_gmv,
                max(tmp.gmv)                         max_gmv,
                sum(tmp.gmv) / count(tmp.mct_id)     avg_gmv,
                sum(tmp.avg_person_price) / count(*) avg_avg_person_price,
                sum(tmp.pay_uv)                      pay_uv
         from (
                  select a.first_cat_name,
                         c.rank,
                         a.mct_id,
                         sum(a.shop_price * a.goods_number + a.shipping_fee) gmv,
                         sum(a.shop_price * a.goods_number + a.shipping_fee) /
                         count(distinct order_goods_id)                      avg_person_price,
                         count(distinct device_id)                           pay_uv

                  from dwd.dwd_vova_fact_pay a
                           join dim.dim_vova_goods b on a.goods_id = b.goods_id
                           join ads.ads_vova_mct_rank c on c.mct_id = b.mct_id and c.first_cat_id = b.first_cat_id
                  where to_date(a.pay_time) >= date_sub('${cur_date}', 29) and to_date(a.pay_time) <= '${cur_date}'
                    and c.pt = '${cur_date}'
                  group by a.first_cat_name, c.rank, a.mct_id
              ) tmp
         group by cube (tmp.first_cat_name, tmp.rank)
) b on a.first_cat_name = b.first_cat_name and a.rank = b.rank
         left join (
    select nvl(tmp.first_cat_name, 'all')    first_cat_name,
           nvl(tmp.rank, 'all')              rank,
           sum(tmp.rep_rate_1mth) / count(*) avg_rep_rate_1mth

    from (
             select t1.mct_id,
                    t1.first_cat_name,
                    t1.rank,
                    count(distinct t2.buyer_id_1) / count(distinct t1.buyer_id) as rep_rate_1mth
             from (
                      select distinct fp.goods_id,
                                      b.mct_id,
                                      b.first_cat_name,
                                      c.rank,
                                      buyer_id
                      from dwd.dwd_vova_fact_pay fp
                          join dim.dim_vova_goods b on fp.goods_id = b.goods_id
                               join ads.ads_vova_mct_rank c on c.mct_id = b.mct_id and c.first_cat_id = b.first_cat_id
                      where year(fp.pay_time) = year('${pre_2_month}')
                        and month(fp.pay_time) = month('${pre_2_month}')
                        and c.pt = '${cur_date}'
                  ) t1
                      left join
                  (
                      select distinct buyer_id as buyer_id_1
                      from dwd.dwd_vova_fact_pay
                      where year(pay_time) = year('${pre_month}')
                        and month(pay_time) = month('${pre_month}')
                  ) t2 on t1.buyer_id = t2.buyer_id_1
             group by t1.mct_id, t1.first_cat_name, t1.rank
         ) tmp
    group by cube (tmp.first_cat_name, tmp.rank)
) c on a.first_cat_name = c.first_cat_name and a.rank = c.rank
         left join (
--step10 5到8周非物流退款率
    select nvl(tmp.first_cat_name, 'all')     first_cat_name,
           nvl(tmp.rank, 'all')               rank,
           sum(tmp.nlrf_rate_5_8w) / count(*) avg_nlrf_rate_5_8w

    from (
             select t1.mct_id,
                    t1.first_cat_name,
                    t1.rank,
                    sum(t1.nlrf_order_cnt_5_8w) / count(t1.order_goods_id) as nlrf_rate_5_8w
             from (
                      select og.mct_id,
                             b.first_cat_name,
                             d.rank,
                             og.order_goods_id,
                             case
                                 when fr.refund_reason_type_id != 8 and fr.refund_type_id = 2 then 1
                                 else 0 end nlrf_order_cnt_5_8w
                      from dim.dim_vova_order_goods og
                               join dim.dim_vova_goods b on og.goods_id = b.goods_id
                               join ads.ads_vova_mct_rank d on d.mct_id = b.mct_id and d.first_cat_id = b.first_cat_id
                               left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id = og.order_goods_id
                               left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id = fl.order_goods_id
                               left join dim.dim_vova_category c on og.cat_id = c.cat_id
                      where datediff('${cur_date}', date(og.confirm_time)) between 35 and 56
                        and og.sku_pay_status > 1
                        and og.sku_shipping_status > 0
                        and d.pt = '${cur_date}'
                  ) t1
             group by t1.mct_id, t1.first_cat_name, t1.rank
         ) tmp
    group by cube (tmp.first_cat_name, tmp.rank)
) d on a.first_cat_name = d.first_cat_name and a.rank = d.rank
         left join (
--step11 9到12周物流退款率
    select nvl(tmp.first_cat_name, 'all')     first_cat_name,
           nvl(tmp.rank, 'all')               rank,
           sum(tmp.lrf_rate_9_12w) / count(*) avg_lrf_rate_9_12w

    from (
             select t1.mct_id,
                    t1.first_cat_name,
                    t1.rank,
                    sum(t1.lrf_order_cnt_9_12w) / count(t1.order_goods_id) as lrf_rate_9_12w
             from (
                      select og.mct_id,
                             b.first_cat_name,
                             d.rank,
                             og.order_goods_id,
                             case
                                 when fr.refund_reason_type_id = 8 and fr.refund_type_id = 2 then 1
                                 else 0 end lrf_order_cnt_9_12w
                      from dim.dim_vova_order_goods og
                               join dim.dim_vova_goods b on og.goods_id = b.goods_id
                               join ads.ads_vova_mct_rank d on d.mct_id = b.mct_id and d.first_cat_id = b.first_cat_id
                               left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id = og.order_goods_id
                               left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id = fl.order_goods_id
                               left join dim.dim_vova_category c on og.cat_id = c.cat_id
                      where datediff('${cur_date}', date(og.confirm_time)) between 63 and 84
                        and og.sku_pay_status > 1
                        and og.sku_shipping_status > 0
                        and d.pt = '${cur_date}'
                  ) t1
             group by t1.mct_id, t1.first_cat_name, t1.rank
         ) tmp
    group by cube (tmp.first_cat_name, tmp.rank)
) e on a.first_cat_name = e.first_cat_name and a.rank = e.rank
         left join (
        select nvl(tmp.first_cat_name, 'all')       first_cat_name,
                nvl(tmp.rank, 'all')                 rank,
                sum(tmp.avg_person_price) / count(*) avg_avg_person_price,
                sum(tmp.pay_uv)                      pay_uv
         from (
                  select a.first_cat_name,
                         c.rank,
                         a.mct_id,
                         sum(a.shop_price * a.goods_number + a.shipping_fee) gmv,
                         sum(a.shop_price * a.goods_number + a.shipping_fee) /
                         count(distinct order_goods_id)                      avg_person_price,
                         count(distinct device_id)                           pay_uv

                  from dwd.dwd_vova_fact_pay a
                           join dim.dim_vova_goods b on a.goods_id = b.goods_id
                           join ads.ads_vova_mct_rank c on c.mct_id = b.mct_id and c.first_cat_id = b.first_cat_id
                  where to_date(a.pay_time) = '${cur_date}'
                    and c.pt = '${cur_date}'
                  group by a.first_cat_name, c.rank, a.mct_id
              ) tmp
         group by cube (tmp.first_cat_name, tmp.rank)
) f on a.first_cat_name = f.first_cat_name and a.rank = f.rank

"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_mct_rank_data" \
--conf "spark.default.parallelism = 430" \
--conf "spark.sql.shuffle.partitions=430" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.crossJoin.enabled=true" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



