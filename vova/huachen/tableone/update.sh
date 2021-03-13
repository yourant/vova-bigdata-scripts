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
  SELECT mct_id,
           first_cat_name,
           count(goods_id) AS goods_can_sale_cnt
    FROM (
             SELECT dg.mct_id,
                    dg.first_cat_name,
                    gcm.goods_id
             FROM (SELECT goods_id
                   FROM ods_vova_vts.ods_vova_goods_on_sale_record
                   WHERE action = 'on' and to_date(create_time)<= '${cur_date}'
                   GROUP BY goods_id) gcm
                      LEFT JOIN dim.dim_vova_goods dg ON gcm.goods_id = dg.goods_id
         ) gal
    GROUP BY mct_id,
             first_cat_name
),
-- 近一个月上新商品数
tmp_online_goods_new_cnt as (
    select mct_id,
                    first_cat_name,
                    count(1) as goods_online_new_cnt
             from (
                      SELECT dg.mct_id,
                             dg.first_cat_name,
                             dg.goods_id
                      FROM (SELECT goods_id
                            FROM ods_vova_vts.ods_vova_goods_on_sale_record
                            WHERE action = 'on'
                              AND datediff(create_time
                                , '${cur_date}')<= 30) gcm
                               INNER JOIN dim.dim_vova_goods dg ON gcm.goods_id = dg.goods_id
                  ) tmp_online_goods_new
             group by mct_id,
                      first_cat_name
),
-- 近一个月上新商品出单数
tmp_goods_new_sold_cnt as (
    SELECT mct_id,
           first_cat_name,
           count(goods_id) AS goods_sold_new_cnt
    FROM (
             SELECT dg.mct_id,
                    dg.first_cat_name,
                    dg.goods_id
             FROM (SELECT goods_id
                   FROM ods_vova_vts.ods_vova_goods_on_sale_record
                   WHERE action = 'on'
                     AND datediff(create_time
                       , '${cur_date}')<= 30) gcm
                      INNER JOIN dim.dim_vova_goods dg ON gcm.goods_id = dg.goods_id
         ) tmp_online_goods_new
    WHERE goods_id IN (SELECT goods_id
                       FROM dwd.dwd_vova_fact_pay
                       WHERE datediff(pay_time,'${cur_date}') <= 30
                       GROUP BY goods_id)
    GROUP BY mct_id,
             first_cat_name
),
-- 加购商品数
tmp_add_cart as (
     select dg.mct_id,
            dg.first_cat_name,
            sum(if(cc.element_name = 'pdAddToCartSuccess', 1, 0)) as add_cat_cnt
     from dwd.dwd_vova_log_common_click cc
              inner join dim.dim_vova_goods dg
                         on dg.virtual_goods_id = cast(cc.element_id as bigint)
     where cc.pt = '${cur_date}'
       and cc.dp = 'vova'
     group by dg.mct_id,
              dg.first_cat_name
)，
--加购商品UV
tmp_cart_uv as (
     select b.mct_id,
            b.first_cat_name,
            count(distinct a.device_id) as cart_uv
     from dwd.dwd_vova_log_common_click a
              join dim.dim_vova_goods b on cast(a.element_id as bigint) = b.virtual_goods_id
     where a.pt = '${cur_date}'
       and a.dp = 'vova'
       and a.element_name = 'pdAddToCartSuccess'
     group by b.mct_id,
              b.first_cat_name
),
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
)
  insert overwrite table tmp.merchant_data partition(event_date='${cur_date}')
SELECT dog.mct_id,
       m.mct_name,
       dog.first_cat_name,
       sum(dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee)                         AS mct_gmv,
       sum(dog.goods_number)                                                                     as goods_number,
       sum(if(dog.sku_shipping_status >= 1, dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee,
              0))                                                                                as mct_gmv_shipped,
       sum(if(dog.sku_shipping_status >= 1, dog.goods_number, 0))                                as goods_number_shipped,
       sum(dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee) / sum(dog.goods_number) as price,
       nvl(sc.goods_sold_cnt, 0) / nvl(csc.goods_can_sale_cnt, 0) * 100                          as goods_sold_rate,
       if(nvl(tmp_online_goods_new_cnt.goods_online_new_cnt, 0) = 0, 0,
          nvl(tmp_goods_new_sold_cnt.goods_sold_new_cnt, 0) / nvl(tmp_online_goods_new_cnt.goods_online_new_cnt, 0)) *
       100                                                                                       as goods_new_sold_rate,
       nvl(tmp_add_cart.add_cat_cnt, 0)                                                          as add_cat_cnt,
       (nvl(tmp_cart_uv.cart_uv, 0) / nvl(tmp_ex_uv.expre_uv, 0)) * 100                             cart_rate
FROM dim.dim_vova_order_goods dog
         left join dim.dim_vova_merchant m on m.mct_id = dog.mct_id
         left join sc
         on sc.mct_id = dog.mct_id and sc.first_cat_name = dog.first_cat_name
         left join csc
         on csc.mct_id = dog.mct_id and csc.first_cat_name = dog.first_cat_name
         left join tmp_online_goods_new_cnt
         on tmp_online_goods_new_cnt.mct_id = dog.mct_id and tmp_online_goods_new_cnt.first_cat_name = dog.first_cat_name
         left join tmp_goods_new_sold_cnt
         on tmp_goods_new_sold_cnt.mct_id = dog.mct_id
         and
         tmp_goods_new_sold_cnt.first_cat_name = dog.first_cat_name
         left join tmp_add_cart
         on tmp_add_cart.mct_id = dog.mct_id and tmp_add_cart.first_cat_name = dog.first_cat_name
         left join tmp_cart_uv
         on tmp_cart_uv.mct_id = dog.mct_id and tmp_cart_uv.first_cat_name = dog.first_cat_name
         left join tmp_ex_uv
         on tmp_ex_uv.mct_id = dog.mct_id and tmp_ex_uv.first_cat_name = dog.first_cat_name
WHERE dog.pay_status >= 1
  AND dog.sku_order_status != 5
  and dog.sku_order_status >= 2
  and to_date(dog.pay_time) = '${cur_date}'
group by
    dog.mct_id,
    m.mct_name,
    dog.first_cat_name,
    nvl(sc.goods_sold_cnt, 0) / nvl(csc.goods_can_sale_cnt, 0) * 100 ,
    if(nvl(tmp_online_goods_new_cnt.goods_online_new_cnt, 0)=0, 0, nvl(tmp_goods_new_sold_cnt.goods_sold_new_cnt, 0)/nvl(tmp_online_goods_new_cnt.goods_online_new_cnt, 0))*100,
    nvl(tmp_add_cart.add_cat_cnt, 0),
    (nvl (tmp_cart_uv.cart_uv, 0)/ nvl (tmp_ex_uv.expre_uv, 0))*100
order by
    dog.mct_id,
    m.mct_name,
    dog.first_cat_name,
    goods_sold_rate,
    goods_new_sold_rate,
    add_cat_cnt,
    cart_rate
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