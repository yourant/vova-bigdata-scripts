select vg.mct_id                    as mct_id,
       vg.first_cat_name            as first_cat_name,
       count(distinct vga.goods_id) as goods_can_sale_cnt
from ods_vova_vts.ods_vova_goods_arc vga
         left join dim.dim_vova_goods vg on vg.goods_id = vga.goods_id
where vga.pt = '2021-01-01'
  and vga.is_on_sale = 1
group by vg.mct_id,
         vg.first_cat_name
order by vg.mct_id,
         vg.first_cat_name
;