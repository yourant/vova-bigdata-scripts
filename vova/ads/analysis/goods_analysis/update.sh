#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
with tmp_clk as(
select
    gc.datasource,
    gc.device_id,
    dg.goods_id,
    gc.page_code,
    gc.list_type,
    count(*) as clk_cnt
from dwd.dwd_vova_log_goods_click gc
inner join dim.dim_vova_goods dg on gc.virtual_goods_id = dg.virtual_goods_id
where pt='${pre_date}'
group by gc.datasource,gc.device_id,dg.goods_id,gc.page_code,gc.list_type
),
tmp_imp as(
select
    gi.datasource,
    gi.device_id,
    dg.goods_id,
    gi.page_code,
    gi.list_type,
    count(*) as expre_cnt
from dwd.dwd_vova_log_goods_impression gi
inner join dim.dim_vova_goods dg on gi.virtual_goods_id = dg.virtual_goods_id
where pt='${pre_date}'
group by gi.datasource,gi.device_id,dg.goods_id,gi.page_code,gi.list_type
),
tmp_caused_ord as(
select
    oc.datasource,
    oc.device_id,
    oc.goods_id,
    oc.pre_page_code as page_code,
    oc.pre_list_type as list_type,
    sum(fp.shop_price*fp.goods_number+fp.shipping_fee) as gmv,
    sum(fp.goods_number) as slaes_vol
from
dwd.dwd_vova_fact_order_cause_v2 oc
inner join dwd.dwd_vova_fact_pay fp
on oc.order_goods_id = fp.order_goods_id and date(fp.pay_time)='${pre_date}'
where pt='${pre_date}'
group by oc.datasource,oc.device_id,oc.goods_id,oc.pre_page_code,oc.pre_list_type
),

tmp_not_caused_ord as(
select
    fp.datasource,
    fp.device_id,
    fp.goods_id,
    'filler' as page_code,
    'filler' as list_type,
    sum(fp.shop_price*fp.goods_number+fp.shipping_fee) as gmv,
    sum(fp.goods_number) as slaes_vol
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time)='${pre_date}'
and not exists (select 1 from dwd.dwd_vova_fact_order_cause_v2 oc where oc.pt= '${pre_date}' and fp.order_goods_id = oc.order_goods_id)
group by fp.datasource,fp.device_id,fp.goods_id
),

tmp_add_cart as(
select
    cc.datasource,
    cc.device_id,
    dg.goods_id,
    cc.pre_page_code as page_code,
    cc.pre_list_type as list_type,
    count(*) as add_cart_cnt
from
dwd.dwd_vova_fact_cart_cause_v2 cc
inner join dim.dim_vova_goods dg on cc.virtual_goods_id = dg.virtual_goods_id
where cc.pt='${pre_date}'
group by cc.datasource,cc.device_id,dg.goods_id,cc.pre_page_code,cc.pre_list_type
)

insert overwrite table ads.ads_vova_goods_analysis partition(pt='${pre_date}')
select
        tmp.datasource,
        tmp.device_id,
        dd.current_buyer_id as buyer_id,
        dd.region_code as geo_country,
        dg.goods_id,
        tmp.page_code,
        tmp.list_type,
        nvl(tmp.clk_cnt,0) as clk_cnt,
        nvl(tmp.expre_cnt,0) as expre_cnt,
        nvl(tmp.slaes_vol,0) as slaes_vol,
        nvl(tmp.gmv,0) as gmv,
        dg.shop_price,
        dg.shipping_fee,
        if(dg.brand_id>0,'Y','N') as is_brand,
        dg.first_cat_name,
        dg.first_cat_id,
        dg.second_cat_name,
        dg.second_cat_id,
        dg.third_cat_name,
        dg.third_cat_id,
        dg.fourth_cat_name,
        dg.fourth_cat_id,
        dg.mct_id,
        dg.mct_name,
        nvl(tmp.add_cart_cnt,0) as add_cart_cnt,
        dg.goods_sn
FROM
    (select
        datasource,
        device_id,
        goods_id,
        page_code,
        list_type,
        sum(clk_cnt) as clk_cnt,
        sum(expre_cnt) as expre_cnt,
        sum(gmv) as gmv,
        sum(slaes_vol) as slaes_vol,
        sum(add_cart_cnt) as add_cart_cnt
        FROM
        (select
            datasource,
            device_id,
            goods_id,
            page_code,
            list_type,
            clk_cnt,
            0 as gmv,
            0 as slaes_vol,
            0 as add_cart_cnt,
            0 as expre_cnt
        FROM
        tmp_clk

        union all

        select
            datasource,
            device_id,
            goods_id,
            page_code,
            list_type,
            0 as clk_cnt,
            gmv,
            slaes_vol,
            0 as add_cart_cnt,
            0 as expre_cnt
        FROM
        tmp_caused_ord

        union all

        select
            datasource,
            device_id,
            goods_id,
            page_code,
            list_type,
            0 as clk_cnt,
            gmv,
            slaes_vol,
            0 as add_cart_cnt,
            0 as expre_cnt
        FROM
        tmp_not_caused_ord


        union all

        select
            datasource,
            device_id,
            goods_id,
            page_code,
            list_type,
            0 as clk_cnt,
            0 as gmv,
            0 as slaes_vol,
            add_cart_cnt,
            0 as expre_cnt
        FROM
        tmp_add_cart

      union all

      select
          datasource,
          device_id,
          goods_id,
          page_code,
          list_type,
          0 as clk_cnt,
          0 as gmv,
          0 slaes_vol,
          0 as add_cart_cnt,
          expre_cnt
        from
      tmp_imp

        )
      group by datasource,device_id,goods_id,page_code,list_type

) tmp
    left join dim.dim_vova_devices dd on tmp.device_id = dd.device_id and tmp.datasource = dd.datasource
    left join dim.dim_vova_goods dg on tmp.goods_id = dg.goods_id
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_goods_analysis" \
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
