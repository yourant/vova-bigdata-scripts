-- 构建用户-品类偏好，离线，按天计算

with tmp_buyer_goods_rating as(
 select /*+ REPARTITION(10) */
       buyer_id,
       gs_id               as goods_id,
       max(first_cat_id)   as first_cat_id,
       max(second_cat_id)  as second_cat_id,
       max(cat_id)         as cat_id,
       max(if (clk_cnt>0, 1, 0))            as is_clk_cnt,
       max(if (collect_cnt>0, 1, 0))        as is_collect,
       max(if (add_cat_cnt>0, 1, 0))        as is_add_cart,
       max(if (ord_cnt>0, 1, 0))            as is_ord,
       max(
          case when t1.ord_cnt>0 then {ord_weight}
               when t1.add_cat_cnt>0 then {add_cart_weight}
               when t1.collect_cnt>0 then {collect_weight}
               when t1.clk_cnt>0 then {clk_weight}
               else 0 end
       )
                                            as cur_rating
    from dws.dws_vova_buyer_goods_behave t1 where pt='{pt}' and clk_cnt+collect_cnt+add_cat_cnt+ord_cnt > 0
    group by buyer_id, gs_id
)
--累?~J| ?~H??~N??~\~I?~A~O好中
insert overwrite table mlb.mlb_vova_buyer_goods_rating_d partition(pt='{pt}')
select /*+ REPARTITION(10) */
   nvl(t1.buyer_id, t2.buyer_id)               as buyer_id,
   nvl(t1.goods_id, t2.goods_id)               as goods_id,
   nvl(t2.first_cat_id, t1.first_cat_id)       as first_cat_id,
   nvl(t2.second_cat_id, t1.second_cat_id)     as second_cat_id,
   nvl(t2.cat_id, t1.cat_id)                   as cat_id,
   nvl(t2.cur_rating, 0)                       as cur_rating,
   round(nvl(t1.his_rating, 0) * {decay} + nvl(t2.cur_rating, 0), 4)
                                               as his_rating
from (select * from mlb.mlb_vova_buyer_goods_rating_d where pt='{pt_before1}')    t1
full join tmp_buyer_goods_rating          t2   on t1.buyer_id = t2.buyer_id and t1.goods_id=t2.goods_id
join (select buyer_id from ads.ads_vova_buyer_portrait_feature where pt>='{pt_before1}' and pt<='{pt}' and (buyer_act ='high_act_user' or buyer_act='mid_act_user' or buyer_act='low_act_user') and buyer_id > 0 group by buyer_id) t4  on nvl(t1.buyer_id, t2.buyer_id) = t4.buyer_id
;

insert overwrite table mlb.mlb_vova_buyer_goods_rating_offline
select /*+ REPARTITION(10) */
   buyer_id      ,
   goods_id      ,
   first_cat_id  ,
   second_cat_id ,
   cat_id        ,
   his_rating * 0.6 as his_rating
from (  
   select
      *, row_number() over(partition by buyer_id order by his_rating desc) as rk
   from (
	   select t1.* from mlb.mlb_vova_buyer_goods_rating_d t1
	   join ads.ads_vova_goods_portrait t2 on t1.pt='{pt}' and t2.pt='{pt}' and t2.is_recommend=1 and t1.goods_id=t2.goods_id and his_rating>=0.11
   )t
)t
where rk<=200
;
