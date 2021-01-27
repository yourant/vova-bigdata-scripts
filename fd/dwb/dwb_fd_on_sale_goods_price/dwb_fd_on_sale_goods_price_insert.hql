insert overwrite table dwb.dwb_fd_on_sale_goods_price partition (pt = '${pt}')
select
       /*+ REPARTITION(1) */
       gpsp.goods_id,
       gpsp.virtual_goods_id,
       gpsp.project_name,
       gpsp.cat_id,
       c.second_cat_id,
       c.second_cat_name,
       gpsp.shop_price_usd,
       gpsp.purchase_price_rmb
from dwd.dwd_fd_goods_purchase_shop_price gpsp
         left join dim.dim_fd_category c on c.cat_id = gpsp.cat_id
where gpsp.pt = '${pt}'
  and is_on_sale = TRUE;