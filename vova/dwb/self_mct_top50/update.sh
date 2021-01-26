#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.maxExecutors=100" --conf "spark.app.name=dwb_vova_self_mct_top50" -e "

INSERT OVERWRITE TABLE dwb.dwb_vova_self_mct_top50 PARTITION (pt = '${cur_date}')
select
'${cur_date}' cur_date,
       a.virtual_goods_id,
       a.is_brand,
       b.expre_pv,
       c.clk_pv,
       b.expre_uv,
       c.clk_uv,
       a.gmv,
       d.cart_uv,
       e.order_uv,
       e.order_cnt,
       e.goods_num,
       a.pay_order_cnt,
       a.pay_uv

from
(
    select b.virtual_goods_id,
           if(b.brand_id >0,'Y','N') is_brand,
           count(distinct a.device_id)                     pay_uv,
           count(distinct a.order_goods_id)                pay_order_cnt,
           sum(a.shop_price * a.goods_number + a.shipping_fee) gmv
    from dwd.dwd_vova_fact_pay a
    join dim.dim_vova_goods b on a.goods_id = b.goods_id
    where a.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
      and to_date(a.pay_time) = '${cur_date}'
      and a.datasource = 'vova'
    group by b.virtual_goods_id,if(b.brand_id >0,'Y','N')
    order by gmv desc
    limit 50
) a
left join
    (
        select
            a.virtual_goods_id,
            if(b.brand_id >0,'Y','N') is_brand,
            count(1) expre_pv,
            count(distinct a.device_id) expre_uv
        from dwd.dwd_vova_log_goods_impression a
        join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
        where a.pt = '${cur_date}' and a.dp = 'vova'
        and b.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
        group by a.virtual_goods_id,if(b.brand_id >0,'Y','N')
        ) b
on a.virtual_goods_id = b.virtual_goods_id
and a.is_brand = b.is_brand
left join
    (
        select
            a.virtual_goods_id,
            if(b.brand_id >0,'Y','N') is_brand,
            count(1) clk_pv,
            count(distinct a.device_id) clk_uv
        from dwd.dwd_vova_log_goods_click a
        join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
        where a.pt = '${cur_date}' and a.dp = 'vova'
        and b.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
        group by a.virtual_goods_id,if(b.brand_id >0,'Y','N')
        ) c
on a.virtual_goods_id = c.virtual_goods_id
and a.is_brand = c.is_brand
left join
    (
        select
            cast(a.element_id as bigint) virtual_goods_id,
            if(b.brand_id >0,'Y','N') is_brand,
            count(distinct a.device_id) cart_uv
        from dwd.dwd_vova_log_common_click a
        join dim.dim_vova_goods b on cast(a.element_id as bigint) = b.virtual_goods_id
        where a.pt = '${cur_date}' and a.dp = 'vova'
        and a.page_code='product_detail' and a.element_name='pdAddToCartSuccess'
        and b.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
        group by cast(a.element_id as bigint),if(b.brand_id >0,'Y','N')
        ) d
on a.virtual_goods_id = d.virtual_goods_id
and a.is_brand = d.is_brand
left join
    (
        select
            b.virtual_goods_id,
            if(b.brand_id >0,'Y','N') is_brand,
            count(distinct a.device_id) order_uv,
            count(distinct a.order_goods_id) order_cnt,
            sum(goods_number) goods_num
        from dim.dim_vova_order_goods a
        join dim.dim_vova_goods b on a.goods_id = b.goods_id
        where a.datasource = 'vova'
        and b.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
        and to_date(a.order_time) = '${cur_date}'
        group by b.virtual_goods_id,if(b.brand_id >0,'Y','N')
        ) e
on a.virtual_goods_id = e.virtual_goods_id
and a.is_brand = e.is_brand
;



INSERT OVERWRITE TABLE dwb.dwb_vova_self_mct_top50_his PARTITION (pt = '${cur_date}')
select
'${cur_date}' cur_date,
       a.virtual_goods_id,
       a.is_brand,
       b.expre_pv,
       c.clk_pv,
       b.expre_uv,
       c.clk_uv,
       a.gmv,
       d.cart_uv,
       e.order_uv,
       e.order_cnt,
       e.goods_num,
       a.pay_order_cnt,
       a.pay_uv

from
(
    select b.virtual_goods_id,
           if(b.brand_id >0,'Y','N') is_brand,
           count(distinct a.device_id)                     pay_uv,
           count(distinct a.order_goods_id)                pay_order_cnt,
           sum(a.shop_price * a.goods_number + a.shipping_fee) gmv
    from dwd.dwd_vova_fact_pay a
    join dim.dim_vova_goods b on a.goods_id = b.goods_id
    where a.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
      and to_date(a.pay_time) >= date_sub('${cur_date}',30) and to_date(a.pay_time) <= '${cur_date}'
      and a.datasource = 'vova'
    group by b.virtual_goods_id,if(b.brand_id >0,'Y','N')
    order by gmv desc
    limit 50
) a
left join
    (
        select
            a.virtual_goods_id,
            if(b.brand_id >0,'Y','N') is_brand,
            count(1) expre_pv,
            count(distinct a.device_id) expre_uv
        from dwd.dwd_vova_log_goods_impression a
        join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
        where a.pt >= date_sub('${cur_date}',30)  and a.dp = 'vova' and a.pt <= '${cur_date}'
        and b.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
        group by a.virtual_goods_id,if(b.brand_id >0,'Y','N')
        ) b
on a.virtual_goods_id = b.virtual_goods_id
and a.is_brand = b.is_brand
left join
    (
        select
            a.virtual_goods_id,
            if(b.brand_id >0,'Y','N') is_brand,
            count(1) clk_pv,
            count(distinct a.device_id) clk_uv
        from dwd.dwd_vova_log_goods_click a
        join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
        where a.pt >= date_sub('${cur_date}',30)  and a.dp = 'vova' and a.pt <= '${cur_date}'
        and b.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
        group by a.virtual_goods_id,if(b.brand_id >0,'Y','N')
        ) c
on a.virtual_goods_id = c.virtual_goods_id
and a.is_brand = c.is_brand
left join
    (
        select
            cast(a.element_id as bigint) virtual_goods_id,
            if(b.brand_id >0,'Y','N') is_brand,
            count(distinct a.device_id) cart_uv
        from dwd.dwd_vova_log_common_click a
        join dim.dim_vova_goods b on cast(a.element_id as bigint) = b.virtual_goods_id
        where a.pt >= date_sub('${cur_date}',30)  and a.dp = 'vova' and a.pt <= '${cur_date}'
        and a.page_code='product_detail' and a.element_name='pdAddToCartSuccess'
        and b.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
        group by cast(a.element_id as bigint),if(b.brand_id >0,'Y','N')
        ) d
on a.virtual_goods_id = d.virtual_goods_id
and a.is_brand = d.is_brand
left join
    (
        select
            b.virtual_goods_id,
            if(b.brand_id >0,'Y','N') is_brand,
            count(distinct a.device_id) order_uv,
            count(distinct a.order_goods_id) order_cnt,
            sum(goods_number) goods_num
        from dim.dim_vova_order_goods a
        join dim.dim_vova_goods b on a.goods_id = b.goods_id
        where a.datasource = 'vova'
        and to_date(a.order_time) >= date_sub('${cur_date}',30) and to_date(a.order_time) <= '${cur_date}'
        group by b.virtual_goods_id,if(b.brand_id >0,'Y','N')
        ) e
on a.virtual_goods_id = e.virtual_goods_id
and a.is_brand = e.is_brand
;

"
if [ $? -ne 0 ];then
  exit 1
fi



