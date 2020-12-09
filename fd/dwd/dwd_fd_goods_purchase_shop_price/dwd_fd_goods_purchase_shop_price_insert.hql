insert overwrite table dwd.dwd_fd_goods_purchase_shop_price partition (pt='${pt}')
select gp.goods_id,
       gp.virtual_goods_id,
       fp.name as project_name,
       g.cat_id,
       gp.shop_price,
       g.purchase_price,
       is_on_sale
from ods_fd_dmc.ods_fd_dmc_goods_project gp
         left join ods_fd_dmc.ods_fd_dmc_goods g on gp.goods_id = g.goods_id
         left join ods_fd_fam.ods_fd_fam_party fp on fp.party_id = gp.party_id;