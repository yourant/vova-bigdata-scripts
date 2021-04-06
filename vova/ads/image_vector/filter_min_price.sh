sql="
drop table tmp.ads_image_vector_source;
CREATE TABLE IF NOT EXISTS tmp.ads_image_vector_source
select
/*+ REPARTITION(4) */
s.vector_id,
s.img_id,
s.goods_id,
t.goods_id t_goods_id,
t.min_price_goods_id,
if(s.goods_id =t.min_price_goods_id,1,0) match,
t.group_number
from ads.ads_image_vector_source s
left join (select goods_id,min_price_goods_id,group_number from ads.ads_min_price_goods_h where pt='2020-12-23' and strategy='e') t on s.goods_id = t.goods_id
where s.pt='2020-12-18';


drop table tmp.ads_image_vector_source_res;
CREATE TABLE IF NOT EXISTS tmp.ads_image_vector_source_res
select
/*+ REPARTITION(4) */
vector_id,
img_id,
goods_id,
t_goods_id,
min_price_goods_id,
match,
group_number from tmp.ads_image_vector_source where t_goods_id is null
union all
select
/*+ REPARTITION(4) */
vector_id,
img_id,
goods_id,
t_goods_id,
min_price_goods_id,
match,
group_number
from tmp.ads_image_vector_source where match =1
union all
select
/*+ REPARTITION(4) */
vector_id,
img_id,
goods_id,
t_goods_id,
min_price_goods_id,
match,
t.group_number
from tmp.ads_image_vector_source t left join (select group_number from tmp.ads_image_vector_source  where match =1 group by group_number) t1 on t.group_number = t1.group_number
where t1.group_number is null and t.t_goods_id is not null;

INSERT overwrite TABLE ads.ads_image_vector_target_d partition(pt='2020-12-22')
select
/*+ REPARTITION(20) */
t.vector_id,
t.img_id,
t.goods_id,
t.class_id,
t.img_url,
t.vector_base64,
t.pt event_date,
t.sku_id,
t.cat_id,
nvl(t.first_cat_id,0) first_cat_id,
nvl(t.second_cat_id,0) second_cat_id,
t.brand_id,
0 is_delete,
1 is_on_sale,
0 is_update
from ads.ads_image_vector_source t
join tmp.ads_image_vector_source_res t1 on t.vector_id=t1.vector_id
where t.pt='2020-12-18';


"


s="\

INSERT overwrite TABLE ads.ads_image_vector_target_d partition(pt='$cur_date')
select
/*+ REPARTITION(4) */
vector_id,
img_id,
goods_id,
class_id,
img_url,
vector_base64,
'$pt'event_date,
sku_id,
cat_id,
nvl(first_cat_id,0) first_cat_id,
nvl(second_cat_id,0) second_cat_id,
brand_id,
0 is_delete,
1 is_on_sale,
0 is_update
from ads.ads_image_vector_source
where pt='$pt'"