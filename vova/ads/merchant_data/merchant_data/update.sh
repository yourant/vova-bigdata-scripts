#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

sql="
--每日出单商品数
with tmp_goods_sold as (
select mct_id,
       nvl(first_cat_id,-1) as first_cat_id,
       goods_sold_cnt
from (
         SELECT mct_id,
                first_cat_id,
                count(distinct goods_id) AS goods_sold_cnt
         FROM dwd.dwd_vova_fact_pay
         WHERE to_date(pay_time) = '${cur_date}'
           and datasource = 'vova'
           and first_cat_id >0
           and mct_id > 0
         GROUP BY mct_id,
                  first_cat_id
             grouping sets (
(mct_id),
(mct_id, first_cat_id)
)) a
),
--每日在架商品数量
tmp_online_goods as (
select
    mct_id,
    nvl(first_cat_id,-1) as first_cat_id,
    goods_can_sale_cnt
from (
         select mct_id,
                first_cat_id,
                count(goods_id) as goods_can_sale_cnt
         from dim.dim_vova_goods
         where is_on_sale=1
           and first_cat_id >0
           and mct_id > 0
         group by mct_id,
                  first_cat_id
             grouping sets (
(mct_id),
(mct_id, first_cat_id)
)
     ) aa
),
-- 上新商品表
tmp_online_goods_new as (
SELECT dg.mct_id,
       dg.first_cat_id,
       dg.goods_id
from (
         select goods_id,
                create_time
         from (
                  select gc.goods_id,
                         gc.create_time,
                         row_number() over(partition by gc.goods_id order by gc.create_time asc) rk
                    from ods_vova_vts.ods_vova_goods_on_sale_record gc
                  where gc.action = 'on'
              )
         where rk = 1
     ) gcm
         INNER JOIN dim.dim_vova_goods dg ON gcm.goods_id = dg.goods_id
WHERE datediff('${cur_date}', gcm.create_time) <= 30
  AND dg.mct_id > 0
  AND dg.first_cat_id > 0
),
-- 近一个月上新商品数
tmp_online_goods_new_cnt as (
select
    mct_id,
    nvl(first_cat_id,-1) as first_cat_id,
    goods_online_new_cnt
from
    (
        select mct_id,
               first_cat_id,
               count(*) as goods_online_new_cnt
        from tmp_online_goods_new
        group by mct_id,
                 first_cat_id
            grouping sets (
          (mct_id),
          (mct_id, first_cat_id)
          )
    ) a
),
-- 近一个月上新商品出单数
tmp_goods_new_sold_cnt as (
select
    mct_id,
    nvl(first_cat_id,-1) as first_cat_id,
    goods_sold_new_cnt
from
    (
        SELECT mct_id,
               first_cat_id,
               count(tmp_online_goods_new.goods_id) AS goods_sold_new_cnt
        FROM (SELECT goods_id
              FROM dwd.dwd_vova_fact_pay
              WHERE datediff('${cur_date}',pay_time) <= 30
              GROUP BY goods_id) as pay
                 inner join tmp_online_goods_new
                            on pay.goods_id = tmp_online_goods_new.goods_id
        GROUP BY mct_id,
                 first_cat_id
            grouping sets (
    (mct_id),
    (mct_id, first_cat_id)
    )
    ) a
),
-- 加购商品数&加购商品UV
tmp_add_cart as (
select
    mct_id,
    nvl(first_cat_id,-1) as first_cat_id,
    add_cat_cnt,
    cart_uv
from
    (
        select dg.mct_id,
               dg.first_cat_id,
               count(1) as add_cat_cnt,
               count(distinct cc.device_id) as cart_uv
        from dwd.dwd_vova_log_common_click cc
                 inner join dim.dim_vova_goods dg
                            on dg.virtual_goods_id = cast(cc.element_id as bigint)
        where cc.pt = '${cur_date}'
          and cc.dp = 'vova'
          and cc.element_name = 'pdAddToCartSuccess'
          and dg.mct_id > 0
          and dg.first_cat_id > 0
        group by dg.mct_id,
                 dg.first_cat_id
            grouping sets (
    (dg.mct_id),
    (dg.mct_id, dg.first_cat_id)
    )
    ) a
),
--商品曝光UV
tmp_ex_uv as (
select
    mct_id,
    nvl(first_cat_id,-1) as first_cat_id,
    expre_uv
from
    (
        select b.mct_id,
               b.first_cat_id,
               count(distinct a.device_id) as expre_uv
        from dwd.dwd_vova_log_goods_impression a
                 join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
        where a.pt = '${cur_date}'
          and a.dp = 'vova'
          and b.mct_id > 0
          and b.first_cat_id > 0
        group by b.mct_id,
                 b.first_cat_id
            grouping sets (
    (b.mct_id),
    (b.mct_id, b.first_cat_id)
    )
    ) a
),
--基础商家数据表
tmp_merchant as (
select
    mct_id,
    nvl(first_cat_id,-1) as first_cat_id,
    mct_gmv,
    goods_order_number,
    mct_gmv_shipped,
    goods_number_shipped
from
    (
        SELECT dog.mct_id                                                        as mct_id,
               g.first_cat_id                                                as first_cat_id,
               sum(dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee) AS mct_gmv,
               count(dog.order_goods_id)                                         as goods_order_number,
               sum(if(dog.sku_shipping_status >= 1, dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee,
                      0))                                                        as mct_gmv_shipped,
               sum(if(dog.sku_shipping_status >= 1, 1, 0))                       as goods_number_shipped
        FROM dim.dim_vova_order_goods dog
                 INNER join dim.dim_vova_goods g
                            on g.goods_id = dog.goods_id
        WHERE dog.sku_order_status >= 1
  AND to_date(dog.confirm_time) = '${cur_date}'
  AND dog.datasource = 'vova'
  AND g.first_cat_id > 0
  AND dog.mct_id > 0
        GROUP BY
            dog.mct_id,
            g.first_cat_id
            grouping sets (
            (dog.mct_id),
            (dog.mct_id, g.first_cat_id)
            )
    ) a
)
insert overwrite table ads.ads_vova_mct_perf_d partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
       tmp_merchant.mct_id                                                                         as mct_id,
       tmp_merchant.first_cat_id                                                                   as first_cat_id,
       to_date('${cur_date}')                                                                      as count_date,
       tmp_merchant.mct_gmv                                                                        as mct_gvm,
       tmp_merchant.goods_order_number                                                             as goods_order_number,
       tmp_merchant.mct_gmv_shipped                                                                as mct_gvm_shipped,
       tmp_merchant.goods_number_shipped                                                           as goods_order_number_shipped,
       nvl(tmp_merchant.mct_gmv / tmp_merchant.goods_order_number, 0)                              as price,
       nvl(tmp_goods_sold.goods_sold_cnt/ tmp_online_goods.goods_can_sale_cnt, 0) as goods_sold_rate,
       nvl(tmp_goods_new_sold_cnt.goods_sold_new_cnt/ tmp_online_goods_new_cnt.goods_online_new_cnt,
           0)                                                                                      as goods_new_sold_rate,
       nvl(tmp_add_cart.add_cat_cnt, 0)                                                            as add_cart_cnt,
       nvl(tmp_add_cart.cart_uv / tmp_ex_uv.expre_uv, 0)                           as cart_rate
FROM tmp_merchant
         left join tmp_goods_sold
                   on tmp_goods_sold.mct_id = tmp_merchant.mct_id and
                      tmp_goods_sold.first_cat_id = tmp_merchant.first_cat_id
         left join tmp_online_goods
                   on tmp_online_goods.mct_id = tmp_merchant.mct_id and
                      tmp_online_goods.first_cat_id = tmp_merchant.first_cat_id
         left join tmp_online_goods_new_cnt
                   on tmp_online_goods_new_cnt.mct_id = tmp_merchant.mct_id and
                      tmp_online_goods_new_cnt.first_cat_id = tmp_merchant.first_cat_id
         left join tmp_goods_new_sold_cnt
                   on tmp_goods_new_sold_cnt.mct_id = tmp_merchant.mct_id
                       and
                      tmp_goods_new_sold_cnt.first_cat_id = tmp_merchant.first_cat_id
         left join tmp_add_cart
                   on tmp_add_cart.mct_id = tmp_merchant.mct_id and
                      tmp_add_cart.first_cat_id = tmp_merchant.first_cat_id
         left join tmp_ex_uv
                   on tmp_ex_uv.mct_id = tmp_merchant.mct_id and tmp_ex_uv.first_cat_id = tmp_merchant.first_cat_id
"

spark-sql \
--conf "spark.app.name=ads_vova_mct_perf_d_yushijia" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi