insert overwrite table dwb.dwb_fd_category_price_rate partition(pt='${pt}')
select
      /*+ REPARTITION(1) */
       project_name,
       first_cat_id,
       first_cat_name,
       second_cat_id,
       second_cat_name,
       price_rate,
       count(distinct virtual_goods_id)                              as goods_num,
       sum(goods_number_day)                                         as sales_num,
       sum(purchase_price * goods_number_day)                        as total_purchase_cost_rmb,
       sum(shop_price * goods_number_day)                            as total_sales_volume_usd,
       sum(if(purchase_price > 0, shop_price * goods_number_day, 0)) as adjusted_sales_volume_usd
FROM (
         SELECT
                rgp.project_name as project_name,
                rgp.goods_id as goods_id,
                rgp.virtual_goods_id as virtual_goods_id,
                rgp.purchase_price_rmb as purchase_price,
                rgp.shop_price_usd as shop_price,
                floor(rgp.shop_price_usd * 6.7 / rgp.purchase_price_rmb) as price_rate,
                og.goods_number_day                               as goods_number_day,
                nvl(dc.first_cat_id,"")                          as first_cat_id,
                nvl(dc.first_cat_name,"")                        as first_cat_name,
                nvl(dc.second_cat_id,"")                           as second_cat_id,
                nvl(dc.second_cat_name,"")                         as second_cat_name
         FROM (
            SELECT
                project_name,
                goods_id,
                virtual_goods_id,
                cat_id,
                purchase_price_rmb,
                shop_price_usd
            FROM dwd.dwd_fd_goods_purchase_shop_price
            WHERE pt = '${pt}'
            AND is_on_sale = TRUE
            AND project_name IN ('floryday','airydress','eoschoice')
         ) rgp

         LEFT JOIN dim.dim_fd_category_new dc ON dc.cat_id = rgp.cat_id
         LEFT JOIN (

          SELECT virtual_goods_id,sum(goods_number) as goods_number_day
          FROM(
              SELECT
                  project_name,
                  order_id,
                  pay_time,
                  cat_name,
                  country_code,
                  platform_type,
                  goods_id,
                  virtual_goods_id,
                  goods_number
              FROM dwd.dwd_fd_order_goods
              WHERE date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}'
              AND goods_id is not null
              AND goods_id > 0
              AND pay_status = 2
          )ogg GROUP BY virtual_goods_id

        ) og ON rgp.virtual_goods_id = og.virtual_goods_id

) tmp
GROUP BY project_name, first_cat_id,first_cat_name, second_cat_id, second_cat_name,price_rate;
