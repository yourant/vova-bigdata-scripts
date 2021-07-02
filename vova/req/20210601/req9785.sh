drop table if exists tmp.tmp_zyzheng_req_base1_0609;
create table tmp.tmp_zyzheng_req_base1_0609 as
select
distinct nvl(tmp2.min_price_goods_id, t1.goods_id) AS goods_id
from
(
select
distinct dg.goods_id
from
dim.dim_vova_goods dg
inner join
(
    select
    mr.mct_id,
    mr.first_cat_id
    from
    ads.ads_vova_mct_rank mr
    WHERE mr.rank = 5 AND mr.pt = '2021-06-08'
    union
    select
    mr.mct_id,
    mr.first_cat_id
    from
    ads.ads_vova_six_rank_mct mr
) mr ON mr.mct_id = dg.mct_id AND mr.first_cat_id = dg.first_cat_id
where dg.is_on_sale = 1
) t1
    LEFT JOIN
    (
      SELECT
        mpg.goods_id,
        mpg.min_price_goods_id
      FROM
        ads.ads_vova_min_price_goods_d mpg
      WHERE pt = '2021-06-08'
        AND strategy = 'c'
    ) tmp2 on t1.goods_id = tmp2.goods_id
;

select
count(*),
count(distinct t1.goods_id),
count(distinct if(brand_id>0,t1.goods_id,null)),
count(distinct if(brand_id=0,t1.goods_id,null))
from
tmp.tmp_zyzheng_req_base1_0609 t1
inner join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id
inner join tmp.tmp_zyzheng_req_base2_0609 t2 on t2.goods_id = t1.goods_id
;


drop table if exists tmp.tmp_zyzheng_req_base2_0609;
create table tmp.tmp_zyzheng_req_base2_0609 as
select
distinct nvl(tmp2.min_price_goods_id, t1.goods_id) AS goods_id
from
(
select
distinct dg.goods_id
from
dim.dim_vova_goods dg
inner join ads.ads_vova_goods_portrait gp on gp.gs_id = dg.goods_id
inner join ods_vova_vts.ods_vova_goods_comment vgc on vgc.goods_id = dg.goods_id
where gp.pt = '2021-06-08'
and dg.is_on_sale = 1
and (gp.nlrf_rate_5_8w is null or gp.nlrf_rate_5_8w <0.05)
) t1
    LEFT JOIN
    (
      SELECT
        mpg.goods_id,
        mpg.min_price_goods_id
      FROM
        ads.ads_vova_min_price_goods_d mpg
      WHERE pt = '2021-06-08'
        AND strategy = 'c'
    ) tmp2 on t1.goods_id = tmp2.goods_id
;

select count(*) from ads.ads_vova_goods_portrait where pt = '2021-06-08';



select
dg.first_cat_name,
dg.second_cat_name,
count(distinct if(brand_id>0,t1.goods_id,null)),
count(distinct if(brand_id=0,t1.goods_id,null))
from
(
select goods_id
from
tmp.tmp_zyzheng_req_base1_0609 t1

union

select goods_id
from
tmp.tmp_zyzheng_req_base2_0609
) t1
inner join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id
group by dg.first_cat_name, dg.second_cat_name
;


select
count(*),
count(distinct t1.goods_id),
count(distinct if(brand_id>0,t1.goods_id,null)),
count(distinct if(brand_id=0,t1.goods_id,null))
from
(
select goods_id
from
tmp.tmp_zyzheng_req_base1_0609 t1

union

select goods_id
from
tmp.tmp_zyzheng_req_base2_0609
) t1
inner join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id

;