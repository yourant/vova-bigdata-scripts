#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

sql="
--商品出单数
with sc as (
     SELECT mct_id,
           first_cat_name,
           count(goods_id) AS goods_sold_cnt
    FROM dwd.dwd_vova_fact_pay
    WHERE to_date(pay_time) = '${cur_date}'
    GROUP BY mct_id,
             first_cat_name
),
--在架商品数量
csc as (
select vg.mct_id                    as mct_id,
       vg.first_cat_name            as first_cat_name,
       count(distinct vga.goods_id) as goods_can_sale_cnt
from ods_vova_vts.ods_vova_goods_arc vga
         left join dim.dim_vova_goods vg on vg.goods_id = vga.goods_id
where vga.pt = '${cur_date}'
  and vga.is_on_sale = 1
group by vg.mct_id,
         vg.first_cat_name
order by vg.mct_id,
         vg.first_cat_name
),
-- 上新商品表
tmp_online_goods_new as (
             SELECT dg.mct_id as mct_id,
                    dg.first_cat_name as first_cat_name,
                    dg.goods_id as goods_id
             FROM (SELECT goods_id
                   FROM ods_vova_vts.ods_vova_goods_on_sale_record
                   WHERE action = 'on'
                     AND datediff(create_time
                       , '${cur_date}')<= 30) gcm
                      INNER JOIN dim.dim_vova_goods dg ON gcm.goods_id = dg.goods_id
),
-- 近一个月上新商品数
tmp_online_goods_new_cnt as (
    select mct_id,
                    first_cat_name,
                    count(1) as goods_online_new_cnt
             from tmp_online_goods_new
             group by mct_id,
                      first_cat_name
),
-- 近一个月上新商品出单数
tmp_goods_new_sold_cnt as (
    SELECT mct_id,
           first_cat_name,
           count(goods_id) AS goods_sold_new_cnt
    FROM tmp_online_goods_new
    WHERE goods_id IN (SELECT goods_id
                       FROM dwd.dwd_vova_fact_pay
                       WHERE datediff(pay_time,'${cur_date}') <= 30
                       GROUP BY goods_id)
    GROUP BY mct_id,
             first_cat_name
),
-- 加购商品数&加购商品UV
tmp_add_cart as (
     select dg.mct_id,
            dg.first_cat_name,
            sum(if(cc.element_name = 'pdAddToCartSuccess', 1, 0)) as add_cat_cnt,
            count(distinct a.device_id) as cart_uv
     from dwd.dwd_vova_log_common_click cc
              inner join dim.dim_vova_goods dg
                         on dg.virtual_goods_id = cast(cc.element_id as bigint)
     where cc.pt = '${cur_date}'
       and cc.dp = 'vova'
       and cc.element_name = 'pdAddToCartSuccess'
     group by dg.mct_id,
              dg.first_cat_name
)，
--商品曝光UV
tmp_ex_uv as (
     select b.mct_id,
            b.first_cat_name,
            count(distinct a.device_id) as expre_uv
     from dwd.dwd_vova_log_goods_impression a
              join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
     where a.pt = '${cur_date}'
       and a.dp = 'vova'
     group by b.mct_id,
              b.first_cat_name
),
--基础商家数据表
tmp_merchant as (
SELECT dog.mct_id as mct_id,
       m.mct_name as mct_name,
       vg.first_cat_id,
       dog.first_cat_name as first_cat_name,
       sum(dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee)                         AS mct_gmv,
       sum(dog.order_goods_id)                                                                     as goods_order_number,
       sum(if(dog.sku_shipping_status >= 1, dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee,
              0))                                                                                as mct_gmv_shipped,
       sum(if(dog.sku_shipping_status >= 1, dog.order_goods_id, 0))                                as goods_number_shipped
FROM dim.dim_vova_order_goods dog
left join dim.dim_vova_merchant m on m.mct_id = dog.mct_id
WHERE dog.sku_order_status != 5
  AND dog.sku_order_status >= 2
  AND to_date(dog.confirm_time) = '${cur_date}'
GROUP BY
       dog.mct_id,
       m.mct_name,
       vg.first_cat_id,
       dog.first_cat_name
),
tmp_merchant_rate as (
SELECT dog.mct_id                                            as mct_id,
       dog.mct_name                                          as mct_name,
       dog.first_cat_name                                    as first_cat_name,
       dog.mct_gmv                                           AS mct_gmv,
       dog.goods_order_number                                as goods_order_number,
       dog.mct_gmv_shipped                                   as mct_gmv_shipped,
       dog.goods_number_shipped                              as goods_number_shipped,
       nvl(sc.goods_sold_cnt, 0)                             as goods_sold_cnt,
       nvl(csc.goods_can_sale_cnt, 0)                        as goods_can_sale_cnt,
       nvl(tmp_goods_new_sold_cnt.goods_sold_new_cnt, 0)     as goods_new_sold_cnt,
       nvl(tmp_online_goods_new_cnt.goods_online_new_cnt, 0) as goods_new_can_sale_cnt,
       nvl(tmp_add_cart.add_cat_cnt, 0)                      as add_cat_cnt,
       nvl(tmp_add_cart.cart_uv, 0)                           as cart_uv,
       nvl(tmp_ex_uv.expre_uv, 0)                            as expre_uv
FROM tmp_merchant dog
         left join sc
                   on sc.mct_id = dog.mct_id and sc.first_cat_name = dog.first_cat_name
         left join csc
                   on csc.mct_id = dog.mct_id and csc.first_cat_name = dog.first_cat_name
         left join tmp_online_goods_new_cnt
                   on tmp_online_goods_new_cnt.mct_id = dog.mct_id and
                      tmp_online_goods_new_cnt.first_cat_name = dog.first_cat_name
         left join tmp_goods_new_sold_cnt
                   on tmp_goods_new_sold_cnt.mct_id = dog.mct_id
                       and
                      tmp_goods_new_sold_cnt.first_cat_name = dog.first_cat_name
         left join tmp_add_cart
                   on tmp_add_cart.mct_id = dog.mct_id and tmp_add_cart.first_cat_name = dog.first_cat_name
         left join tmp_ex_uv
                   on tmp_ex_uv.mct_id = dog.mct_id and tmp_ex_uv.first_cat_name = dog.first_cat_name
),
insert overwrite table tmp.merchant_data partition(event_date='${cur_date}')
select mct_id,
       mct_name,
       first_cat_name,
       mct_gmv,
       goods_order_number,
       mct_gmv_shipped,
       goods_number_shipped,
       if(goods_order_number = 0, 0, mct_gmv / goods_order_number)                            as price,
       if(goods_can_sale_cnt = 0, 0, (goods_sold_cnt / goods_can_sale_cnt) * 100)             as goods_sold_rate,
       if(goods_new_can_sale_cnt = 0, 0, (goods_new_sold_cnt / goods_new_can_sale_cnt) * 100) as goods_new_sold_rate,
       add_cat_cnt,
       if(expre_uv = 0, 0, (cart_uv / expre_uv) * 100)                                        as cart_rate
from tmp_merchant_rate
union
select mct_id,
       mct_name,
       'all'                                                                                                 as first_cat_name,
       sum(mct_gmv)                                                                                          as mct_gvm,
       sum(goods_order_number)                                                                               as goods_order_number,
       sum(mct_gmv_shipped)                                                                                  as mct_gmv_shipped,
       sum(goods_number_shipped)                                                                             as goods_number_shipped,
       if(sum(goods_order_number) = 0, 0, sum(mct_gmv) / sum(goods_order_number))                            as price,
       if(sum(goods_can_sale_cnt) = 0, 0,
          (sum(goods_sold_cnt) / sum(goods_can_sale_cnt)) * 100)                                             as goods_sold_rate,
       if(sum(goods_new_can_sale_cnt) = 0, 0,
          (sum(goods_new_sold_cnt) / sum(goods_new_can_sale_cnt)) * 100)                                     as goods_new_sold_rate,
       sum(add_cat_cnt)                                                                                      as add_cat_cnt,
       if(sum(expre_uv) = 0, 0, (sum(cart_uv) / sum(expre_uv)) * 100)                                        as cart_rate
from tmp_merchant_rate
group by mct_id,
         mct_name,
         'all'
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=merchant_data" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi