select
count(*)
from dwd.dwd_vova_fact_pay p
join ods_vova_vteos.ods_vova_goods_gallery gg on p.goods_id = gg.goods_id
join dim.dim_vova_goods g on g.goods_id = p.goods_id
where g.is_on_sale =1  and gg.is_delete=0;
