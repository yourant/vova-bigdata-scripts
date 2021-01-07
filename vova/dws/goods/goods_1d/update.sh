#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table dws.dws_vova_goods_1d PARTITION (pt = '${pre_date}')
select
/*+ COALESCE(30) */
expre_data.datasource as datasource,
expre_data.ctry as ctry,
expre_data.os_type as os_type,
dg.goods_id as gs_id,
expre_data.gender as gender,
nvl(clk_data.clk_uv,0) as clk_uv,
nvl(clk_data.clk_cnt,0) as clk_cnt,
nvl(expre_data.expre_uv,0) as expre_uv,
nvl(expre_data.expre_cnt,0) as expre_cnt,
nvl(addcart_data.add_cart_uv,0) as add_cart_uv,
nvl(addcart_data.add_cart_cnt,0) as add_cart_cnt,
nvl(addcart_data.collect_cnt,0) as collect_cnt,
nvl(clk_data.sr_uv,0) as sr_uv,
nvl(clk_data.sr_cnt,0) as sr_cnt,
nvl(ord_data.ord_uv,0) as ord_uv,
nvl(ord_data.ord_cnt,0) as ord_cnt,
nvl(ord_data.pay_uv,0) as pay_uv,
nvl(ord_data.pay_cnt,0) as pay_cnt,
nvl(ord_data.sales_vol,0) as sales_vol,
nvl(ord_data.gmv,0) as gmv,
dg.shop_price as shop_price ,
dg.cat_id as cat_id,
dg.first_cat_id as first_cat_id,
dg.first_cat_name as first_cat_name
from
    (select
    nvl(datasource,'all') as datasource,
    nvl(ctry,'all') as ctry,
    nvl(os_type,'all') as os_type,
    nvl(gender,'all') as gender,
    nvl(vir_gs_id,'all') as vir_gs_id ,
    count(distinct device_id) as expre_uv,
    count(device_id) as expre_cnt
    from
        (select
        nvl(datasource,'NA') as datasource,
        nvl(country,'NALL') as ctry,
        nvl(os_type,'NA') as os_type,
        nvl(gender,'NA') as gender,
        nvl(virtual_goods_id,'NA') as vir_gs_id ,
        device_id
        from dwd.dwd_vova_log_goods_impression
        where pt='${pre_date}' and platform ='mob' and os_type is not null and os_type !='')
    group by
    vir_gs_id,
    datasource,
    ctry,
    os_type,
    gender
    grouping sets(
      (vir_gs_id,datasource,ctry,os_type,gender),
      (vir_gs_id,datasource,ctry,os_type),
      (vir_gs_id,datasource,ctry,gender),
      (vir_gs_id,datasource,gender,os_type),
      (vir_gs_id,ctry,gender,os_type),
      (vir_gs_id,datasource,ctry),
      (vir_gs_id,datasource,os_type),
      (vir_gs_id,datasource,gender),
      (vir_gs_id,ctry,gender),
      (vir_gs_id,ctry,os_type),
      (vir_gs_id,os_type,gender),
      (vir_gs_id,datasource),
      (vir_gs_id,ctry),
      (vir_gs_id,os_type),
      (vir_gs_id,gender),
      (vir_gs_id)
      ))expre_data
left join
    (select
    nvl(datasource,'all') as datasource,
    nvl(ctry,'all') as ctry,
    nvl(os_type,'all') as os_type,
    nvl(gender,'all') as gender,
    nvl(vir_gs_id,'all') as vir_gs_id ,
    count(distinct device_id) as clk_uv,
    count(*) as clk_cnt,
    count(distinct sr_device_id) as sr_uv,
    count(sr_device_id) as sr_cnt
    from
        (select
        nvl(datasource,'NA') as datasource,
        nvl(country,'NALL') as ctry,
        nvl(os_type,'NA') as os_type,
        nvl(gender,'NA') as gender,
        nvl(virtual_goods_id,'NA') as vir_gs_id ,
        device_id,
        if( page_code = 'search_result',device_id,null) as sr_device_id
        from dwd.dwd_vova_log_goods_click
        where pt='${pre_date}' and platform ='mob' and os_type is not null and os_type !='')
    group by
    vir_gs_id,
    datasource,
    ctry,
    os_type,
    gender
    grouping sets(
      (vir_gs_id,datasource,ctry,os_type,gender),
      (vir_gs_id,datasource,ctry,os_type),
      (vir_gs_id,datasource,ctry,gender),
      (vir_gs_id,datasource,gender,os_type),
      (vir_gs_id,ctry,gender,os_type),
      (vir_gs_id,datasource,ctry),
      (vir_gs_id,datasource,os_type),
      (vir_gs_id,datasource,gender),
      (vir_gs_id,ctry,gender),
      (vir_gs_id,ctry,os_type),
      (vir_gs_id,os_type,gender),
      (vir_gs_id,datasource),
      (vir_gs_id,ctry),
      (vir_gs_id,os_type),
      (vir_gs_id,gender),
      (vir_gs_id)
      ))clk_data
on  clk_data.datasource=expre_data.datasource
and clk_data.ctry=expre_data.ctry
and clk_data.os_type=expre_data.os_type
and clk_data.gender=expre_data.gender
and clk_data.vir_gs_id=expre_data.vir_gs_id
left join
    (select
    nvl(datasource,'all') as datasource,
    nvl(ctry,'all') as ctry,
    nvl(os_type,'all') as os_type,
    nvl(gender,'all') as gender,
    nvl(vir_gs_id,'all') as vir_gs_id ,
    count(distinct add_cat_device_id) as add_cart_uv,
    count(add_cat_device_id) as add_cart_cnt,
    count(collect_device_id) as collect_cnt
    from
        (select
        nvl(datasource,'NA') as datasource,
        nvl(country,'NALL') as ctry,
        nvl(os_type,'NA') as os_type,
        nvl(gender,'NA') as gender,
        nvl(cast(element_id as bigint),'NA') as vir_gs_id,
        case when element_name='pdAddToCartSuccess' then device_id  end as add_cat_device_id,
        case when element_name='pdAddToWishlistClick' then device_id end as collect_device_id
        from dwd.dwd_vova_log_common_click
        where pt='${pre_date}' and platform ='mob' and os_type is not null and os_type !='' and element_name in ('pdAddToCartSuccess','pdAddToWishlistClick'))
    group by
    vir_gs_id,
    datasource,
    ctry,
    os_type,
    gender
    grouping sets(
      (vir_gs_id,datasource,ctry,os_type,gender),
      (vir_gs_id,datasource,ctry,os_type),
      (vir_gs_id,datasource,ctry,gender),
      (vir_gs_id,datasource,gender,os_type),
      (vir_gs_id,ctry,gender,os_type),
      (vir_gs_id,datasource,ctry),
      (vir_gs_id,datasource,os_type),
      (vir_gs_id,datasource,gender),
      (vir_gs_id,ctry,gender),
      (vir_gs_id,ctry,os_type),
      (vir_gs_id,os_type,gender),
      (vir_gs_id,datasource),
      (vir_gs_id,ctry),
      (vir_gs_id,os_type),
      (vir_gs_id,gender),
      (vir_gs_id)
      ))addcart_data
on  clk_data.datasource=addcart_data.datasource
and clk_data.ctry=addcart_data.ctry
and clk_data.os_type=addcart_data.os_type
and clk_data.gender=addcart_data.gender
and clk_data.vir_gs_id=addcart_data.vir_gs_id
inner join (select goods_id,virtual_goods_id,shop_price,cat_id,first_cat_id,first_cat_name from dim.dim_vova_goods) dg on dg.virtual_goods_id = expre_data.vir_gs_id
left join
    (select
    nvl(datasource,'all') as datasource,
    nvl(ctry,'all') as ctry,
    nvl(os_type,'all') as os_type,
    nvl(gender,'all') as gender,
    nvl(gs_id,'all') as gs_id ,
    count(distinct ord_buyer_id) as ord_uv,
    count(ord_buyer_id) as ord_cnt,
    SUM(gmv) as gmv,
    count(distinct pay_buyer_id) as pay_uv,
    count(pay_buyer_id) as pay_cnt,
    sum(goods_number) as sales_vol
    from
        (select
        nvl(dog.datasource,'NA') as datasource,
        nvl(dog.region_code,'NALL') as ctry,
        nvl(dog.platform,'NA') as os_type,
        nvl(dog.gender,'NA') as gender,
        nvl(dog.goods_id,'NA') as gs_id ,
        fp.shop_price * fp.goods_number + fp.shipping_fee as gmv,
        nvl(fp.goods_number,0) as goods_number,
        dog.buyer_id as ord_buyer_id,
        fp.buyer_id as pay_buyer_id
        from
        dim.dim_vova_order_goods dog
        left join dwd.dwd_vova_fact_pay fp
        on dog.order_goods_id = fp.order_goods_id
        where to_date(dog.order_time)='${pre_date}'
        and dog.platform in ('ios','android'))
    group by
    gs_id,
    datasource,
    ctry,
    os_type,
    gender
    grouping sets(
      (gs_id,datasource,ctry,os_type,gender),
      (gs_id,datasource,ctry,os_type),
      (gs_id,datasource,ctry,gender),
      (gs_id,datasource,gender,os_type),
      (gs_id,ctry,gender,os_type),
      (gs_id,datasource,ctry),
      (gs_id,datasource,os_type),
      (gs_id,datasource,gender),
      (gs_id,ctry,gender),
      (gs_id,ctry,os_type),
      (gs_id,os_type,gender),
      (gs_id,datasource),
      (gs_id,ctry),
      (gs_id,os_type),
      (gs_id,gender),
      (gs_id)
      )) ord_data
on  clk_data.datasource=ord_data.datasource
and clk_data.ctry=ord_data.ctry
and clk_data.os_type=ord_data.os_type
and clk_data.gender=ord_data.gender
and dg.goods_id=ord_data.gs_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dws_vova_goods_1d" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
