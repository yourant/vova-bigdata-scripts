insert overwrite table dwb.dwb_fd_on_sale_goods_price partition (pt='${pt}')
select gp.goods_id,
       gp.virtual_goods_id,
       fp.name as project_name,
       g.cat_id,
       c.second_cat_id,
       c.second_cat_name,
       g.purchase_price,
       gp.shop_price
from ods_fd_dmc.ods_fd_dmc_goods_project gp
         left join ods_fd_dmc.ods_fd_dmc_goods g on gp.goods_id = g.goods_id
         left join ods_fd_fam.ods_fd_fam_party fp on fp.party_id = gp.party_id
         left join dim.dim_fd_category c on c.cat_id = g.cat_id
where is_on_sale = TRUE;