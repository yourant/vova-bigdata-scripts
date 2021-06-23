--参数：
--bayes_a      品类偏好贝叶斯参数分子部分, 默认1
--bayes_b      品类偏好贝叶斯参数分母部分, 默认50
--clk_weight   点击权重，默认1
--collect_weight   收藏权重，默认3
--add_cart_weight  加购权重，默认5
--ord_weight       购买权重，默认5
--decay            天衰减系数，默认0.96
--pt               分区，日期，yyyy-MM-dd

with tmp_vova_buyer_cat_rating as
(   select /*+ REPARTITION(10) */
       buyer_id, second_cat_id,
       sum(if (t1.expre_cnt>0, 1, 0))      as cur_expre_cnt,
       sum(if (t1.clk_cnt>0, 1, 0))        as cur_clk_cnt,
       sum(if (t1.collect_cnt>0, 1, 0))    as cur_collect_cnt,
       sum(if (t1.add_cat_cnt>0, 1, 0))    as cur_add_cart_cnt,
       sum(if (t1.ord_cnt>0, 1, 0))        as cur_ord_cnt,
       sum(case when t1.ord_cnt>0 then {ord_weight}
                when t1.add_cat_cnt>0 then {add_cart_weight}
                when t1.collect_cnt>0 then {collect_weight}
                when t1.clk_cnt>0     then {clk_weight}
                else 0 end
             )                            as cur_rating
    from dws.dws_vova_buyer_goods_behave t1 where pt='{pt}' and second_cat_id>0
    group by buyer_id, second_cat_id
)
insert overwrite table mlb.mlb_vova_buyer_cat_rating_d partition(pt='{pt}')
-- calculate users' preference to cat
select /*+ REPARTITION(10) */
   nvl(t1.buyer_id, t2.buyer_id),
   nvl(t1.cat_id, t2.second_cat_id)                                      as cat_id,
   'second_cat_id'                                                       as cat_type,
   nvl(t2.cur_expre_cnt, 0)                                              as cur_expre_cnt,
   nvl(t2.cur_clk_cnt, 0)                                                as cur_clk_cnt,
   nvl(t2.cur_collect_cnt, 0)                                            as cur_collect_cnt,
   nvl(t2.cur_add_cart_cnt, 0)                                           as cur_add_cart_cnt,
   nvl(t2.cur_ord_cnt, 0)                                                as cur_ord_cnt,
   round(nvl(t2.cur_rating, 0), 4)                                                 as cur_rating,
   nvl(t1.his_expre_cnt, 0) + nvl(t2.cur_expre_cnt, 0)                   as his_expre_cnt,
   nvl(t1.his_clk_cnt, 0) + nvl(t2.cur_clk_cnt, 0)                       as his_clk_cnt,
   nvl(t1.his_collect_cnt, 0) + nvl(t2.cur_collect_cnt, 0)               as his_collect_cnt,
   nvl(t1.his_add_cart_cnt, 0) + nvl(t2.cur_add_cart_cnt, 0)             as his_add_cart_cnt,
   nvl(t1.his_ord_cnt, 0) + nvl(t2.cur_ord_cnt, 0)                       as his_ord_cnt,
   round(nvl(t2.cur_rating, 0) + nvl(t1.his_rating, 0) * {decay}, 4)                  as his_rating,
   round(nvl(t1.his_expre_cnt, 0)*{decay} + nvl(t2.cur_expre_cnt, 0), 4)             as his_expre_score,
   10*round((nvl(t2.cur_rating, 0) + nvl(t1.his_rating, 0) * {decay} + {bayes_a}) / (nvl(t1.his_expre_score, 0)*{decay} + nvl(t2.cur_expre_cnt, 0)+{bayes_b}), 4)
                                                                         as pref
from (select * from mlb.mlb_vova_buyer_cat_rating_d where pt='{pt_before1}')  t1
full join tmp_vova_buyer_cat_rating       t2 on t1.buyer_id = t2.buyer_id and t1.cat_type='second_cat_id' and t1.cat_id = t2.second_cat_id
join (select buyer_id from ads.ads_vova_buyer_portrait_feature where pt>='{pt_before1}' and pt<='{pt}' and (buyer_act ='high_act_user' or buyer_act='mid_act_user' or buyer_act='low_act_user') and buyer_id > 0 group by buyer_id) t3 on nvl(t1.buyer_id, t2.buyer_id)=t3.buyer_id
where nvl(t1.his_rating, 0) * {decay} + nvl(t2.cur_rating, 0) > 0   --only keep no zero score
;

--存储结果，过滤掉偏好度低的商品和品类
insert overwrite table mlb.mlb_vova_buyer_cat_rating_offline
select /*+ REPARTITION(10) */
   buyer_id         ,
   cat_id           ,
   cat_type         ,
   his_rating       ,
   his_expre_score  ,
   pref
from mlb.mlb_vova_buyer_cat_rating_d
where pt='{pt}' and pref>=0.083    --曝光20次无点击表示无偏好
;
