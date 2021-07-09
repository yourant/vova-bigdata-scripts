#!/bin/bash
cur_date=$1
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi


spark-sql \
--driver-memory 8G \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.dynamicAllocation.maxExecutors=250" \
--conf "spark.app.name=mlb_vova_goods_rate" \
-e"



insert overwrite table tmp.mlb_goods_rate_01_1d
    select /*+ REPARTITION(50) */ gs_id,
           --近1天
           sum(expre_cnt) expre_cnt_1d,
           count(distinct if(expre_cnt > 0,buyer_id,null)) expre_uv_1d,
           sum(clk_cnt) clk_cnt_1d,
           count(distinct if(clk_cnt > 0 ,buyer_id,null)) clk_uv_1d,
           round((sum(clk_cnt) + 15) / (sum(expre_cnt) + 500),4) clk_rate_1d,
           round((count(distinct if(ord_cnt > 0 ,buyer_id,null)) + 1) / (count(distinct if(expre_cnt > 0 ,buyer_id,null)) + 500),4) cr_rate_1d,
           round(sum(expre_cnt) / count(distinct buyer_id),4) expre_cnt_per_u_1d,
           round(sum(clk_cnt) / count(distinct buyer_id),4) clk_cnt_per_u_1d,
           sum(gmv) gmv_1d,
           sum(collect_cnt) collect_cnt_1d,
           count(distinct if(collect_cnt > 0 ,buyer_id,null)) collect_uv_1d,
           sum(add_cat_cnt) add_cart_cnt_1d,
           count(distinct if(add_cat_cnt > 0 ,buyer_id,null)) add_cart_uv_1d,
           sum(ord_cnt) order_cnt_1d,
           count(distinct if(ord_cnt > 0 ,buyer_id,null)) order_uv_1d,
           sum(sales_vol) sale_vol_1d,

           --近3天
           0 expre_cnt_3d,
           0 expre_uv_3d,
           0 clk_cnt_3d,
           0 clk_uv_3d,
           0 clk_rate_3d,
           0 cr_rate_3d,
           0 expre_cnt_per_u_3d,
           0 clk_cnt_per_u_3d,
           0 gmv_3d,
           0 collect_cnt_3d,
           0 collect_uv_3d,
           0 add_cart_cnt_3d,
           0 add_cart_uv_3d,
           0 order_cnt_3d,
           0 order_uv_3d,
           0 sale_vol_3d,

           --近7天
           0 expre_cnt_7d,
           0 expre_uv_7d,
           0 clk_cnt_7d,
           0 clk_uv_7d,
           0 clk_rate_7d,
           0 cr_rate_7d,
           0 expre_cnt_per_u_7d,
           0 clk_cnt_per_u_7d,
           0 gmv_7d,
           0 collect_cnt_7d,
           0 collect_uv_7d,
           0 add_cart_cnt_7d,
           0 add_cart_uv_7d,
           0 order_cnt_7d,
           0 order_uv_7d,
           0 sale_vol_7d,

           --近14天
           0 expre_cnt_14d,
           0 expre_uv_14d,
           0 clk_cnt_14d,
           0 clk_uv_14d,
           0 clk_rate_14d,
           0 cr_rate_14d,
           0 expre_cnt_per_u_14d,
           0 clk_cnt_per_u_14d,
           0 gmv_14d,
           0 collect_cnt_14d,
           0 collect_uv_14d,
           0 add_cart_cnt_14d,
           0 add_cart_uv_14d,
           0 order_cnt_14d,
           0 order_uv_14d,
           0 sale_vol_14d,


           --近30天
           0 expre_cnt_30d,
           0 expre_uv_30d,
           0 clk_cnt_30d,
           0 clk_uv_30d,
           0 clk_rate_30d,
           0 cr_rate_30d,
           0 expre_cnt_per_u_30d,
           0 clk_cnt_per_u_30d,
           0 gmv_30d,
           0 collect_cnt_30d,
           0 collect_uv_30d,
           0 add_cart_cnt_30d,
           0 add_cart_uv_30d,
           0 order_cnt_30d,
           0 order_uv_30d,
           0 sale_vol_30d

    from dws.dws_vova_buyer_goods_behave a where a.pt = date_sub('${cur_date}',1)
    group by gs_id
;

insert overwrite table  tmp.mlb_goods_rate_01_3d

select /*+ REPARTITION(50) */  gs_id,
           --近1天
           0 expre_cnt_1d,
           0 expre_uv_1d,
           0 clk_cnt_1d,
           0 clk_uv_1d,
           0 clk_rate_1d,
           0 cr_rate_1d,
           0 expre_cnt_per_u_1d,
           0 clk_cnt_per_u_1d,
           0 gmv_1d,
           0 collect_cnt_1d,
           0 collect_uv_1d,
           0 add_cart_cnt_1d,
           0 add_cart_uv_1d,
           0 order_cnt_1d,
           0 order_uv_1d,
           0 sale_vol_1d,

           --近3天
           sum(expre_cnt) expre_cnt_3d,
           count(distinct if(expre_cnt > 0,buyer_id,null)) expre_uv_3d,
           sum(clk_cnt) clk_cnt_3d,
           count(distinct if(clk_cnt > 0 ,buyer_id,null)) clk_uv_3d,
           round((sum(clk_cnt) + 15) / (sum(expre_cnt) + 500),4) clk_rate_3d,
           round((count(distinct if(ord_cnt > 0 ,buyer_id,null)) + 1) / (count(distinct if(expre_cnt > 0 ,buyer_id,null)) + 500),4) cr_rate_3d,
           round(sum(expre_cnt) / count(distinct buyer_id),4) expre_cnt_per_u_3d,
           round(sum(clk_cnt) / count(distinct buyer_id),4) clk_cnt_per_u_3d,
           sum(gmv) gmv_3d,
           sum(collect_cnt) collect_cnt_3d,
           count(distinct if(collect_cnt > 0 ,buyer_id,null)) collect_uv_3d,
           sum(add_cat_cnt) add_cart_cnt_3d,
           count(distinct if(add_cat_cnt > 0 ,buyer_id,null)) add_cart_uv_3d,
           sum(ord_cnt) order_cnt_3d,
           count(distinct if(ord_cnt > 0 ,buyer_id,null)) order_uv_3d,
           sum(sales_vol) sale_vol_3d,

           --近7天
           0 expre_cnt_7d,
           0 expre_uv_7d,
           0 clk_cnt_7d,
           0 clk_uv_7d,
           0 clk_rate_7d,
           0 cr_rate_7d,
           0 expre_cnt_per_u_7d,
           0 clk_cnt_per_u_7d,
           0 gmv_7d,
           0 collect_cnt_7d,
           0 collect_uv_7d,
           0 add_cart_cnt_7d,
           0 add_cart_uv_7d,
           0 order_cnt_7d,
           0 order_uv_7d,
           0 sale_vol_7d,

           --近14天
           0 expre_cnt_14d,
           0 expre_uv_14d,
           0 clk_cnt_14d,
           0 clk_uv_14d,
           0 clk_rate_14d,
           0 cr_rate_14d,
           0 expre_cnt_per_u_14d,
           0 clk_cnt_per_u_14d,
           0 gmv_14d,
           0 collect_cnt_14d,
           0 collect_uv_14d,
           0 add_cart_cnt_14d,
           0 add_cart_uv_14d,
           0 order_cnt_14d,
           0 order_uv_14d,
           0 sale_vol_14d,


           --近30天
           0 expre_cnt_30d,
           0 expre_uv_30d,
           0 clk_cnt_30d,
           0 clk_uv_30d,
           0 clk_rate_30d,
           0 cr_rate_30d,
           0 expre_cnt_per_u_30d,
           0 clk_cnt_per_u_30d,
           0 gmv_30d,
           0 collect_cnt_30d,
           0 collect_uv_30d,
           0 add_cart_cnt_30d,
           0 add_cart_uv_30d,
           0 order_cnt_30d,
           0 order_uv_30d,
           0 sale_vol_30d


    from dws.dws_vova_buyer_goods_behave a where a.pt >= date_sub('${cur_date}',3) and a.pt <= date_sub('${cur_date}',1)
    group by gs_id

;


insert overwrite table   tmp.mlb_goods_rate_01_7d
select /*+ REPARTITION(50) */  gs_id,
           --近1天
           0 expre_cnt_1d,
           0 expre_uv_1d,
           0 clk_cnt_1d,
           0 clk_uv_1d,
           0 clk_rate_1d,
           0 cr_rate_1d,
           0 expre_cnt_per_u_1d,
           0 clk_cnt_per_u_1d,
           0 gmv_1d,
           0 collect_cnt_1d,
           0 collect_uv_1d,
           0 add_cart_cnt_1d,
           0 add_cart_uv_1d,
           0 order_cnt_1d,
           0 order_uv_1d,
           0 sale_vol_1d,

           --近3天
           0 expre_cnt_3d,
           0 expre_uv_3d,
           0 clk_cnt_3d,
           0 clk_uv_3d,
           0 clk_rate_3d,
           0 cr_rate_3d,
           0 expre_cnt_per_u_3d,
           0 clk_cnt_per_u_3d,
           0 gmv_3d,
           0 collect_cnt_3d,
           0 collect_uv_3d,
           0 add_cart_cnt_3d,
           0 add_cart_uv_3d,
           0 order_cnt_3d,
           0 order_uv_3d,
           0 sale_vol_3d,

           --近7天
           sum(expre_cnt) expre_cnt_7d,
           count(distinct if(expre_cnt > 0,buyer_id,null)) expre_uv_7d,
           sum(clk_cnt) clk_cnt_7d,
           count(distinct if(clk_cnt > 0 ,buyer_id,null)) clk_uv_7d,
           round((sum(clk_cnt) + 15) / (sum(expre_cnt) + 500),4) clk_rate_7d,
           round((count(distinct if(ord_cnt > 0 ,buyer_id,null)) + 1) / (count(distinct if(expre_cnt > 0 ,buyer_id,null)) + 500),4) cr_rate_7d,
           round(sum(expre_cnt) / count(distinct buyer_id),4) expre_cnt_per_u_7d,
           round(sum(clk_cnt) / count(distinct buyer_id),4) clk_cnt_per_u_7d,
           sum(gmv) gmv_7d,
           sum(collect_cnt) collect_cnt_7d,
           count(distinct if(collect_cnt > 0 ,buyer_id,null)) collect_uv_7d,
           sum(add_cat_cnt) add_cart_cnt_7d,
           count(distinct if(add_cat_cnt > 0 ,buyer_id,null)) add_cart_uv_7d,
           sum(ord_cnt) order_cnt_7d,
           count(distinct if(ord_cnt > 0 ,buyer_id,null)) order_uv_7d,
           sum(sales_vol) sale_vol_7d,

           --近14天
           0 expre_cnt_14d,
           0 expre_uv_14d,
           0 clk_cnt_14d,
           0 clk_uv_14d,
           0 clk_rate_14d,
           0 cr_rate_14d,
           0 expre_cnt_per_u_14d,
           0 clk_cnt_per_u_14d,
           0 gmv_14d,
           0 collect_cnt_14d,
           0 collect_uv_14d,
           0 add_cart_cnt_14d,
           0 add_cart_uv_14d,
           0 order_cnt_14d,
           0 order_uv_14d,
           0 sale_vol_14d,


           --近30天
           0 expre_cnt_30d,
           0 expre_uv_30d,
           0 clk_cnt_30d,
           0 clk_uv_30d,
           0 clk_rate_30d,
           0 cr_rate_30d,
           0 expre_cnt_per_u_30d,
           0 clk_cnt_per_u_30d,
           0 gmv_30d,
           0 collect_cnt_30d,
           0 collect_uv_30d,
           0 add_cart_cnt_30d,
           0 add_cart_uv_30d,
           0 order_cnt_30d,
           0 order_uv_30d,
           0 sale_vol_30d


    from dws.dws_vova_buyer_goods_behave a where a.pt >= date_sub('${cur_date}',7) and a.pt <= date_sub('${cur_date}',1)
    group by gs_id

;

insert overwrite table   tmp.mlb_goods_rate_01_14d
select /*+ REPARTITION(50) */  gs_id,
           --近1天
           0 expre_cnt_1d,
           0 expre_uv_1d,
           0 clk_cnt_1d,
           0 clk_uv_1d,
           0 clk_rate_1d,
           0 cr_rate_1d,
           0 expre_cnt_per_u_1d,
           0 clk_cnt_per_u_1d,
           0 gmv_1d,
           0 collect_cnt_1d,
           0 collect_uv_1d,
           0 add_cart_cnt_1d,
           0 add_cart_uv_1d,
           0 order_cnt_1d,
           0 order_uv_1d,
           0 sale_vol_1d,

           --近3天
           0 expre_cnt_3d,
           0 expre_uv_3d,
           0 clk_cnt_3d,
           0 clk_uv_3d,
           0 clk_rate_3d,
           0 cr_rate_3d,
           0 expre_cnt_per_u_3d,
           0 clk_cnt_per_u_3d,
           0 gmv_3d,
           0 collect_cnt_3d,
           0 collect_uv_3d,
           0 add_cart_cnt_3d,
           0 add_cart_uv_3d,
           0 order_cnt_3d,
           0 order_uv_3d,
           0 sale_vol_3d,

           --近7天
           0 expre_cnt_7d,
           0 expre_uv_7d,
           0 clk_cnt_7d,
           0 clk_uv_7d,
           0 clk_rate_7d,
           0 cr_rate_7d,
           0 expre_cnt_per_u_7d,
           0 clk_cnt_per_u_7d,
           0 gmv_7d,
           0 collect_cnt_7d,
           0 collect_uv_7d,
           0 add_cart_cnt_7d,
           0 add_cart_uv_7d,
           0 order_cnt_7d,
           0 order_uv_7d,
           0 sale_vol_7d,

           --近14天
           sum(expre_cnt) expre_cnt_14d,
           count(distinct if(expre_cnt > 0,buyer_id,null)) expre_uv_14d,
           sum(clk_cnt) clk_cnt_14d,
           count(distinct if(clk_cnt > 0 ,buyer_id,null)) clk_uv_14d,
           round((sum(clk_cnt) + 15) / (sum(expre_cnt) + 500),4) clk_rate_14d,
           round((count(distinct if(ord_cnt > 0 ,buyer_id,null)) + 1) / (count(distinct if(expre_cnt > 0 ,buyer_id,null)) + 500),4) cr_rate_14d,
           round(sum(expre_cnt) / count(distinct buyer_id),4) expre_cnt_per_u_14d,
           round(sum(clk_cnt) / count(distinct buyer_id),4) clk_cnt_per_u_14d,
           sum(gmv) gmv_14d,
           sum(collect_cnt) collect_cnt_14d,
           count(distinct if(collect_cnt > 0 ,buyer_id,null)) collect_uv_14d,
           sum(add_cat_cnt) add_cart_cnt_14d,
           count(distinct if(add_cat_cnt > 0 ,buyer_id,null)) add_cart_uv_14d,
           sum(ord_cnt) order_cnt_14d,
           count(distinct if(ord_cnt > 0 ,buyer_id,null)) order_uv_14d,
           sum(sales_vol) sale_vol_14d,


           --近30天
           0 expre_cnt_30d,
           0 expre_uv_30d,
           0 clk_cnt_30d,
           0 clk_uv_30d,
           0 clk_rate_30d,
           0 cr_rate_30d,
           0 expre_cnt_per_u_30d,
           0 clk_cnt_per_u_30d,
           0 gmv_30d,
           0 collect_cnt_30d,
           0 collect_uv_30d,
           0 add_cart_cnt_30d,
           0 add_cart_uv_30d,
           0 order_cnt_30d,
           0 order_uv_30d,
           0 sale_vol_30d


    from dws.dws_vova_buyer_goods_behave a where a.pt >= date_sub('${cur_date}',14) and a.pt <= date_sub('${cur_date}',1)
    group by gs_id

;




insert overwrite table   tmp.mlb_goods_rate_01_30d
select /*+ REPARTITION(50) */  gs_id,
           --近1天
           0 expre_cnt_1d,
           0 expre_uv_1d,
           0 clk_cnt_1d,
           0 clk_uv_1d,
           0 clk_rate_1d,
           0 cr_rate_1d,
           0 expre_cnt_per_u_1d,
           0 clk_cnt_per_u_1d,
           0 gmv_1d,
           0 collect_cnt_1d,
           0 collect_uv_1d,
           0 add_cart_cnt_1d,
           0 add_cart_uv_1d,
           0 order_cnt_1d,
           0 order_uv_1d,
           0 sale_vol_1d,

           --近3天
           0 expre_cnt_3d,
           0 expre_uv_3d,
           0 clk_cnt_3d,
           0 clk_uv_3d,
           0 clk_rate_3d,
           0 cr_rate_3d,
           0 expre_cnt_per_u_3d,
           0 clk_cnt_per_u_3d,
           0 gmv_3d,
           0 collect_cnt_3d,
           0 collect_uv_3d,
           0 add_cart_cnt_3d,
           0 add_cart_uv_3d,
           0 order_cnt_3d,
           0 order_uv_3d,
           0 sale_vol_3d,

           --近7天
           0 expre_cnt_7d,
           0 expre_uv_7d,
           0 clk_cnt_7d,
           0 clk_uv_7d,
           0 clk_rate_7d,
           0 cr_rate_7d,
           0 expre_cnt_per_u_7d,
           0 clk_cnt_per_u_7d,
           0 gmv_7d,
           0 collect_cnt_7d,
           0 collect_uv_7d,
           0 add_cart_cnt_7d,
           0 add_cart_uv_7d,
           0 order_cnt_7d,
           0 order_uv_7d,
           0 sale_vol_7d,

           --近14天
           0 expre_cnt_14d,
           0 expre_uv_14d,
           0 clk_cnt_14d,
           0 clk_uv_14d,
           0 clk_rate_14d,
           0 cr_rate_14d,
           0 expre_cnt_per_u_14d,
           0 clk_cnt_per_u_14d,
           0 gmv_14d,
           0 collect_cnt_14d,
           0 collect_uv_14d,
           0 add_cart_cnt_14d,
           0 add_cart_uv_14d,
           0 order_cnt_14d,
           0 order_uv_14d,
           0 sale_vol_14d,


           --近30天
           sum(expre_cnt) expre_cnt_30d,
           count(distinct if(expre_cnt > 0,buyer_id,null)) expre_uv_30d,
           sum(clk_cnt) clk_cnt_30d,
           count(distinct if(clk_cnt > 0 ,buyer_id,null)) clk_uv_30d,
           round((sum(clk_cnt) + 15) / (sum(expre_cnt) + 500),4) clk_rate_30d,
           round((count(distinct if(ord_cnt > 0 ,buyer_id,null)) + 1) / (count(distinct if(expre_cnt > 0 ,buyer_id,null)) + 500),4) cr_rate_30d,
           round(sum(expre_cnt) / count(distinct buyer_id),4) expre_cnt_per_u_30d,
           round(sum(clk_cnt) / count(distinct buyer_id),4) clk_cnt_per_u_30d,
           sum(gmv) gmv_30d,
           sum(collect_cnt) collect_cnt_30d,
           count(distinct if(collect_cnt > 0 ,buyer_id,null)) collect_uv_30d,
           sum(add_cat_cnt) add_cart_cnt_30d,
           count(distinct if(add_cat_cnt > 0 ,buyer_id,null)) add_cart_uv_30d,
           sum(ord_cnt) order_cnt_30d,
           count(distinct if(ord_cnt > 0 ,buyer_id,null)) order_uv_30d,
           sum(sales_vol) sale_vol_30d


    from dws.dws_vova_buyer_goods_behave a where a.pt >= date_sub('${cur_date}',30) and a.pt <= date_sub('${cur_date}',1)
    group by gs_id

;

insert overwrite table tmp.mlb_goods_rate_01
select /*+ REPARTITION(50) */
gs_id,
             --近1天
           sum(expre_cnt_1d),
           sum(expre_uv_1d),
           sum(clk_cnt_1d),
           sum(clk_uv_1d),
           sum(clk_rate_1d),
           sum(cr_rate_1d),
           sum(expre_cnt_per_u_1d),
           sum(clk_cnt_per_u_1d),
           sum(gmv_1d),
           sum(collect_cnt_1d),
           sum(collect_uv_1d),
           sum(add_cart_cnt_1d),
           sum(add_cart_uv_1d),
           sum(order_cnt_1d),
           sum(order_uv_1d),
           sum(sale_vol_1d),
           --近3天
           sum(expre_cnt_3d),
           sum(expre_uv_3d),
           sum(clk_cnt_3d),
           sum(clk_uv_3d),
           sum(clk_rate_3d),
           sum(cr_rate_3d),
           sum(expre_cnt_per_u_3d),
           sum(clk_cnt_per_u_3d),
           sum(gmv_3d),
           sum(collect_cnt_3d),
           sum(collect_uv_3d),
           sum(add_cart_cnt_3d),
           sum(add_cart_uv_3d),
           sum(order_cnt_3d),
           sum(order_uv_3d),
           sum(sale_vol_3d),
           --近7天
           sum(expre_cnt_7d),
           sum(expre_uv_7d),
           sum(clk_cnt_7d),
           sum(clk_uv_7d),
           sum(clk_rate_7d),
           sum(cr_rate_7d),
           sum(expre_cnt_per_u_7d),
           sum(clk_cnt_per_u_7d),
           sum(gmv_7d),
           sum(collect_cnt_7d),
           sum(collect_uv_7d),
           sum(add_cart_cnt_7d),
           sum(add_cart_uv_7d),
           sum(order_cnt_7d),
           sum(order_uv_7d),
           sum(sale_vol_7d),
           --近14天
           sum(expre_cnt_14d),
           sum(expre_uv_14d),
           sum(clk_cnt_14d),
           sum(clk_uv_14d),
           sum(clk_rate_14d),
           sum(cr_rate_14d),
           sum(expre_cnt_per_u_14d),
           sum(clk_cnt_per_u_14d),
           sum(gmv_14d),
           sum(collect_cnt_14d),
           sum(collect_uv_14d),
           sum(add_cart_cnt_14d),
           sum(add_cart_uv_14d),
           sum(order_cnt_14d),
           sum(order_uv_14d),
           sum(sale_vol_14d),


             --近30天
           sum(expre_cnt_30d),
           sum(expre_uv_30d),
           sum(clk_cnt_30d),
           sum(clk_uv_30d),
           sum(clk_rate_30d),
           sum(cr_rate_30d),
           sum(expre_cnt_per_u_30d),
           sum(clk_cnt_per_u_30d),
           sum(gmv_30d),
           sum(collect_cnt_30d),
           sum(collect_uv_30d),
           sum(add_cart_cnt_30d),
           sum(add_cart_uv_30d),
           sum(order_cnt_30d),
           sum(order_uv_30d),
           sum(sale_vol_30d)
from (
         select *
         from tmp.mlb_goods_rate_01_1d
         union all
         select *
         from tmp.mlb_goods_rate_01_3d
         union all
         select *
         from tmp.mlb_goods_rate_01_7d
         union all
         select *
         from tmp.mlb_goods_rate_01_14d
         union all
         select *
         from tmp.mlb_goods_rate_01_30d
     ) t
group by t.gs_id
;


insert overwrite table  tmp.mlb_goods_rate_refund
select /*+ REPARTITION(50) */
    og.goods_id,
    --近1天
    count(distinct if(to_date(fr.audit_time) = date_sub('${cur_date}',1), og.order_goods_id,null)) refund_order_cnt_1d,
    sum(if(to_date(fr.audit_time) = date_sub('${cur_date}',1), og.goods_number * og.shop_price + shipping_fee,0)) refund_amt_1d,

       --近3天
    count(distinct if(to_date(fr.audit_time) >= date_sub('${cur_date}',3) and to_date(fr.audit_time) <= date_sub('${cur_date}',1), og.order_goods_id,null)) refund_order_cnt_3d,
    sum(if(to_date(fr.audit_time) >= date_sub('${cur_date}',3) and to_date(fr.audit_time)  <= date_sub('${cur_date}',1), og.goods_number * og.shop_price + shipping_fee,0)) refund_amt_3d,
       --近7天
    count(distinct if(to_date(fr.audit_time) >= date_sub('${cur_date}',7) and to_date(fr.audit_time) <= date_sub('${cur_date}',1), og.order_goods_id,null)) refund_order_cnt_7d,
    sum(if(to_date(fr.audit_time) >= date_sub('${cur_date}',7) and to_date(fr.audit_time)  <= date_sub('${cur_date}',1), og.goods_number * og.shop_price + shipping_fee,0)) refund_amt_7d,

       --近14天
    count(distinct if(to_date(fr.audit_time) >= date_sub('${cur_date}',14) and to_date(fr.audit_time) <= date_sub('${cur_date}',1), og.order_goods_id,null)) refund_order_cnt_14d,
    sum(if(to_date(fr.audit_time) >= date_sub('${cur_date}',14) and to_date(fr.audit_time)  <= date_sub('${cur_date}',1), og.goods_number * og.shop_price + shipping_fee,0)) refund_amt_14d,

       --近30天
    count(distinct if(to_date(fr.audit_time) >= date_sub('${cur_date}',30) and to_date(fr.audit_time) <= date_sub('${cur_date}',1), og.order_goods_id,null)) refund_order_cnt_30d,
    sum(if(to_date(fr.audit_time) >= date_sub('${cur_date}',30) and to_date(fr.audit_time)  <= date_sub('${cur_date}',1), og.goods_number * og.shop_price + shipping_fee,0)) refund_amt_30d


  from dim.dim_vova_order_goods og
  join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
  where to_date(fr.audit_time) > date_sub( '${cur_date}', 30 )
    AND to_date ( fr.audit_time ) <= date_sub('${cur_date}',1) and fr.rr_audit_status = 'audit_passed' and og.sku_pay_status>1
    group by og.goods_id
;


insert overwrite table  tmp.mlb_goods_rate_brand
select  /*+ REPARTITION(100) */
  a.goods_id,b.expre_cnt_1d, clk_cnt_1d, gmv_1d, expre_cnt_3d, clk_cnt_3d, gmv_3d, expre_cnt_7d, clk_cnt_7d, gmv_7d, expre_cnt_14d, clk_cnt_14d, gmv_14d, expre_cnt_30d, clk_cnt_30d, gmv_30d
from dim.dim_vova_goods a
join (
         select brand_id,
                --近1天
                sum(if(a.pt = date_sub('${cur_date}',1),expre_cnt,0)) expre_cnt_1d,
                sum(if(a.pt = date_sub('${cur_date}',1),clk_cnt,0))   clk_cnt_1d,
                sum(if(a.pt = date_sub('${cur_date}',1),gmv,0))       gmv_1d,

                --近3天
                sum(if(a.pt >= date_sub('${cur_date}',3) and a.pt <= date_sub('${cur_date}',1),expre_cnt,0)) expre_cnt_3d,
                sum(if(a.pt >= date_sub('${cur_date}',3) and a.pt <= date_sub('${cur_date}',1),clk_cnt,0))   clk_cnt_3d,
                sum(if(a.pt >= date_sub('${cur_date}',3) and a.pt <= date_sub('${cur_date}',1),gmv,0))       gmv_3d,

                --近7天
                sum(if(a.pt >= date_sub('${cur_date}',7) and a.pt <= date_sub('${cur_date}',1),expre_cnt,0)) expre_cnt_7d,
                sum(if(a.pt >= date_sub('${cur_date}',7) and a.pt <= date_sub('${cur_date}',1),clk_cnt,0))   clk_cnt_7d,
                sum(if(a.pt >= date_sub('${cur_date}',7) and a.pt <= date_sub('${cur_date}',1),gmv,0))       gmv_7d,

                --近14天
                sum(if(a.pt >= date_sub('${cur_date}',14) and a.pt <= date_sub('${cur_date}',1),expre_cnt,0)) expre_cnt_14d,
                sum(if(a.pt >= date_sub('${cur_date}',14) and a.pt <= date_sub('${cur_date}',1),clk_cnt,0))   clk_cnt_14d,
                sum(if(a.pt >= date_sub('${cur_date}',14) and a.pt <= date_sub('${cur_date}',1),gmv,0))       gmv_14d,

                --近30天
                sum(if(a.pt >= date_sub('${cur_date}',30) and a.pt <= date_sub('${cur_date}',1),expre_cnt,0)) expre_cnt_30d,
                sum(if(a.pt >= date_sub('${cur_date}',30) and a.pt <= date_sub('${cur_date}',1),clk_cnt,0))   clk_cnt_30d,
                sum(if(a.pt >= date_sub('${cur_date}',30) and a.pt <= date_sub('${cur_date}',1),gmv,0))       gmv_30d
         from dws.dws_vova_buyer_goods_behave a
         where a.pt >= date_sub('${cur_date}', 30)
           and a.pt <= date_sub('${cur_date}', 1)
         group by brand_id
     ) b on a.brand_id = b.brand_id
;


insert overwrite table   tmp.mlb_goods_rate_second_cat_1d
    select  /*+ REPARTITION(100) */
      a.goods_id,b.expre_cnt_second_1d, expre_uv_second_1d, clk_cnt_second_1d, clk_uv_second_1d, gmv_second_1d, gmv_uv_second_1d, expre_cnt_second_3d, expre_uv_second_3d, clk_cnt_second_3d, clk_uv_second_3d, gmv_second_3d, gmv_uv_second_3d, expre_cnt_second_7d, expre_uv_second_7d, clk_cnt_second_7d, clk_uv_second_7d, gmv_second_7d, gmv_uv_second_7d, expre_cnt_second_14d, expre_uv_second_14d, clk_cnt_second_14d, clk_uv_second_14d, gmv_second_14d, gmv_uv_second_14d, expre_cnt_second_30d, expre_uv_second_30d, clk_cnt_second_30d, clk_uv_second_30d, gmv_second_30d, gmv_uv_second_30d
    from dim.dim_vova_goods a
    join (
             select second_cat_id,
                    --近1天
                    sum(expre_cnt) expre_cnt_second_1d,
                    count(distinct if(a.expre_cnt > 0,a.buyer_id,null)) expre_uv_second_1d,
                    sum(clk_cnt)   clk_cnt_second_1d,
                    count(distinct if(a.clk_cnt > 0,a.buyer_id,null)) clk_uv_second_1d,
                    sum(gmv)       gmv_second_1d,
                    count(distinct if(a.gmv > 0,a.buyer_id,null)) gmv_uv_second_1d,

                    --近3天
                    0 expre_cnt_second_3d,
                    0 expre_uv_second_3d,
                    0 clk_cnt_second_3d,
                    0 clk_uv_second_3d,
                    0 gmv_second_3d,
                    0 gmv_uv_second_3d,

                    --近7天
                    0 expre_cnt_second_7d,
                    0 expre_uv_second_7d,
                    0 clk_cnt_second_7d,
                    0 clk_uv_second_7d,
                    0 gmv_second_7d,
                    0 gmv_uv_second_7d,

                    --近14天
                    0 expre_cnt_second_14d,
                    0 expre_uv_second_14d,
                    0 clk_cnt_second_14d,
                    0 clk_uv_second_14d,
                    0 gmv_second_14d,
                    0 gmv_uv_second_14d,

                    --近30天
                    0 expre_cnt_second_30d,
                    0 expre_uv_second_30d,
                    0 clk_cnt_second_30d,
                    0 clk_uv_second_30d,
                    0 gmv_second_30d,
                    0 gmv_uv_second_30d

             from dws.dws_vova_buyer_goods_behave a
             where a.pt = date_sub('${cur_date}', 1)
             group by second_cat_id
         ) b on a.second_cat_id = b.second_cat_id
;

insert overwrite table  tmp.mlb_goods_rate_second_cat_3d
    select  /*+ REPARTITION(100) */
      a.goods_id,b.expre_cnt_second_1d, expre_uv_second_1d, clk_cnt_second_1d, clk_uv_second_1d, gmv_second_1d, gmv_uv_second_1d, expre_cnt_second_3d, expre_uv_second_3d, clk_cnt_second_3d, clk_uv_second_3d, gmv_second_3d, gmv_uv_second_3d, expre_cnt_second_7d, expre_uv_second_7d, clk_cnt_second_7d, clk_uv_second_7d, gmv_second_7d, gmv_uv_second_7d, expre_cnt_second_14d, expre_uv_second_14d, clk_cnt_second_14d, clk_uv_second_14d, gmv_second_14d, gmv_uv_second_14d, expre_cnt_second_30d, expre_uv_second_30d, clk_cnt_second_30d, clk_uv_second_30d, gmv_second_30d, gmv_uv_second_30d
    from dim.dim_vova_goods a
    join (
             select second_cat_id,
                    --近1天
                    0 expre_cnt_second_1d,
                    0 expre_uv_second_1d,
                    0 clk_cnt_second_1d,
                    0 clk_uv_second_1d,
                    0 gmv_second_1d,
                    0 gmv_uv_second_1d,

                    --近3天
                    sum(expre_cnt) expre_cnt_second_3d,
                    count(distinct if(a.expre_cnt > 0,a.buyer_id,null)) expre_uv_second_3d,
                    sum(clk_cnt)   clk_cnt_second_3d,
                    count(distinct if(a.clk_cnt > 0,a.buyer_id,null)) clk_uv_second_3d,
                    sum(gmv)       gmv_second_3d,
                    count(distinct if(a.gmv > 0,a.buyer_id,null)) gmv_uv_second_3d,

                    --近7天
                    0 expre_cnt_second_7d,
                    0 expre_uv_second_7d,
                    0 clk_cnt_second_7d,
                    0 clk_uv_second_7d,
                    0 gmv_second_7d,
                    0 gmv_uv_second_7d,

                    --近14天
                    0 expre_cnt_second_14d,
                    0 expre_uv_second_14d,
                    0 clk_cnt_second_14d,
                    0 clk_uv_second_14d,
                    0 gmv_second_14d,
                    0 gmv_uv_second_14d,

                    --近30天
                    0 expre_cnt_second_30d,
                    0 expre_uv_second_30d,
                    0 clk_cnt_second_30d,
                    0 clk_uv_second_30d,
                    0 gmv_second_30d,
                    0 gmv_uv_second_30d

             from dws.dws_vova_buyer_goods_behave a
             where a.pt >= date_sub('${cur_date}', 3)
             and a.pt <= date_sub('${cur_date}', 1)
             group by second_cat_id
         ) b on a.second_cat_id = b.second_cat_id
;

insert overwrite table  tmp.mlb_goods_rate_second_cat_7d
    select  /*+ REPARTITION(100) */
      a.goods_id,b.expre_cnt_second_1d, expre_uv_second_1d, clk_cnt_second_1d, clk_uv_second_1d, gmv_second_1d, gmv_uv_second_1d, expre_cnt_second_3d, expre_uv_second_3d, clk_cnt_second_3d, clk_uv_second_3d, gmv_second_3d, gmv_uv_second_3d, expre_cnt_second_7d, expre_uv_second_7d, clk_cnt_second_7d, clk_uv_second_7d, gmv_second_7d, gmv_uv_second_7d, expre_cnt_second_14d, expre_uv_second_14d, clk_cnt_second_14d, clk_uv_second_14d, gmv_second_14d, gmv_uv_second_14d, expre_cnt_second_30d, expre_uv_second_30d, clk_cnt_second_30d, clk_uv_second_30d, gmv_second_30d, gmv_uv_second_30d
    from dim.dim_vova_goods a
    join (
             select second_cat_id,
                    --近1天
                    0 expre_cnt_second_1d,
                    0 expre_uv_second_1d,
                    0 clk_cnt_second_1d,
                    0 clk_uv_second_1d,
                    0 gmv_second_1d,
                    0 gmv_uv_second_1d,

                    --近3天
                    0 expre_cnt_second_3d,
                    0 expre_uv_second_3d,
                    0 clk_cnt_second_3d,
                    0 clk_uv_second_3d,
                    0 gmv_second_3d,
                    0 gmv_uv_second_3d,

                    --近7天
                    sum(expre_cnt) expre_cnt_second_7d,
                    count(distinct if(a.expre_cnt > 0,a.buyer_id,null)) expre_uv_second_7d,
                    sum(clk_cnt)   clk_cnt_second_7d,
                    count(distinct if(a.clk_cnt > 0,a.buyer_id,null)) clk_uv_second_7d,
                    sum(gmv)       gmv_second_7d,
                    count(distinct if(a.gmv > 0,a.buyer_id,null)) gmv_uv_second_7d,

                    --近14天
                    0 expre_cnt_second_14d,
                    0 expre_uv_second_14d,
                    0 clk_cnt_second_14d,
                    0 clk_uv_second_14d,
                    0 gmv_second_14d,
                    0 gmv_uv_second_14d,

                    --近30天
                    0 expre_cnt_second_30d,
                    0 expre_uv_second_30d,
                    0 clk_cnt_second_30d,
                    0 clk_uv_second_30d,
                    0 gmv_second_30d,
                    0 gmv_uv_second_30d

             from dws.dws_vova_buyer_goods_behave a
             where a.pt >= date_sub('${cur_date}', 7)
             and a.pt <= date_sub('${cur_date}', 1)
             group by second_cat_id
         ) b on a.second_cat_id = b.second_cat_id
;


insert overwrite table  tmp.mlb_goods_rate_second_cat_14d
    select  /*+ REPARTITION(100) */
      a.goods_id,b.expre_cnt_second_1d, expre_uv_second_1d, clk_cnt_second_1d, clk_uv_second_1d, gmv_second_1d, gmv_uv_second_1d, expre_cnt_second_3d, expre_uv_second_3d, clk_cnt_second_3d, clk_uv_second_3d, gmv_second_3d, gmv_uv_second_3d, expre_cnt_second_7d, expre_uv_second_7d, clk_cnt_second_7d, clk_uv_second_7d, gmv_second_7d, gmv_uv_second_7d, expre_cnt_second_14d, expre_uv_second_14d, clk_cnt_second_14d, clk_uv_second_14d, gmv_second_14d, gmv_uv_second_14d, expre_cnt_second_30d, expre_uv_second_30d, clk_cnt_second_30d, clk_uv_second_30d, gmv_second_30d, gmv_uv_second_30d
    from dim.dim_vova_goods a
    join (
             select second_cat_id,
                    --近1天
                    0 expre_cnt_second_1d,
                    0 expre_uv_second_1d,
                    0 clk_cnt_second_1d,
                    0 clk_uv_second_1d,
                    0 gmv_second_1d,
                    0 gmv_uv_second_1d,

                    --近3天
                    0 expre_cnt_second_3d,
                    0 expre_uv_second_3d,
                    0 clk_cnt_second_3d,
                    0 clk_uv_second_3d,
                    0 gmv_second_3d,
                    0 gmv_uv_second_3d,

                    --近7天
                    0 expre_cnt_second_7d,
                    0 expre_uv_second_7d,
                    0 clk_cnt_second_7d,
                    0 clk_uv_second_7d,
                    0 gmv_second_7d,
                    0 gmv_uv_second_7d,

                    --近14天
                    sum(expre_cnt) expre_cnt_second_14d,
                    count(distinct if(a.expre_cnt > 0,a.buyer_id,null)) expre_uv_second_14d,
                    sum(clk_cnt)   clk_cnt_second_14d,
                    count(distinct if(a.clk_cnt > 0,a.buyer_id,null)) clk_uv_second_14d,
                    sum(gmv)       gmv_second_14d,
                    count(distinct if(a.gmv > 0,a.buyer_id,null)) gmv_uv_second_14d,

                    --近30天
                    0 expre_cnt_second_30d,
                    0 expre_uv_second_30d,
                    0 clk_cnt_second_30d,
                    0 clk_uv_second_30d,
                    0 gmv_second_30d,
                    0 gmv_uv_second_30d

             from dws.dws_vova_buyer_goods_behave a
             where a.pt >= date_sub('${cur_date}', 14)
             and a.pt <= date_sub('${cur_date}', 1)
             group by second_cat_id
         ) b on a.second_cat_id = b.second_cat_id
;


insert overwrite table  tmp.mlb_goods_rate_second_cat_30d
    select  /*+ REPARTITION(100) */
      a.goods_id,b.expre_cnt_second_1d, expre_uv_second_1d, clk_cnt_second_1d, clk_uv_second_1d, gmv_second_1d, gmv_uv_second_1d, expre_cnt_second_3d, expre_uv_second_3d, clk_cnt_second_3d, clk_uv_second_3d, gmv_second_3d, gmv_uv_second_3d, expre_cnt_second_7d, expre_uv_second_7d, clk_cnt_second_7d, clk_uv_second_7d, gmv_second_7d, gmv_uv_second_7d, expre_cnt_second_14d, expre_uv_second_14d, clk_cnt_second_14d, clk_uv_second_14d, gmv_second_14d, gmv_uv_second_14d, expre_cnt_second_30d, expre_uv_second_30d, clk_cnt_second_30d, clk_uv_second_30d, gmv_second_30d, gmv_uv_second_30d
    from dim.dim_vova_goods a
    join (
             select second_cat_id,
                    --近1天
                    0 expre_cnt_second_1d,
                    0 expre_uv_second_1d,
                    0 clk_cnt_second_1d,
                    0 clk_uv_second_1d,
                    0 gmv_second_1d,
                    0 gmv_uv_second_1d,

                    --近3天
                    0 expre_cnt_second_3d,
                    0 expre_uv_second_3d,
                    0 clk_cnt_second_3d,
                    0 clk_uv_second_3d,
                    0 gmv_second_3d,
                    0 gmv_uv_second_3d,

                    --近7天
                    0 expre_cnt_second_7d,
                    0 expre_uv_second_7d,
                    0 clk_cnt_second_7d,
                    0 clk_uv_second_7d,
                    0 gmv_second_7d,
                    0 gmv_uv_second_7d,

                    --近14天
                    0 expre_cnt_second_14d,
                    0 expre_uv_second_14d,
                    0 clk_cnt_second_14d,
                    0 clk_uv_second_14d,
                    0 gmv_second_14d,
                    0 gmv_uv_second_14d,

                    --近30天
                    sum(expre_cnt) expre_cnt_second_30d,
                    count(distinct if(a.expre_cnt > 0,a.buyer_id,null)) expre_uv_second_30d,
                    sum(clk_cnt)   clk_cnt_second_30d,
                    count(distinct if(a.clk_cnt > 0,a.buyer_id,null)) clk_uv_second_30d,
                    sum(gmv)       gmv_second_30d,
                    count(distinct if(a.gmv > 0,a.buyer_id,null)) gmv_uv_second_30d

             from dws.dws_vova_buyer_goods_behave a
             where a.pt >= date_sub('${cur_date}', 30)
             and a.pt <= date_sub('${cur_date}', 1)
             group by second_cat_id
         ) b on a.second_cat_id = b.second_cat_id
;

insert overwrite table tmp.mlb_goods_rate_second_cat
select
t.goods_id,
                    --近1天
                    sum(expre_cnt_second_1d),
                    sum(expre_uv_second_1d),
                    sum(clk_cnt_second_1d),
                    sum(clk_uv_second_1d),
                    sum(gmv_second_1d),
                    sum(gmv_uv_second_1d),

                    --近3天
                    sum(expre_cnt_second_3d),
                    sum(expre_uv_second_3d),
                    sum(clk_cnt_second_3d),
                    sum(clk_uv_second_3d),
                    sum(gmv_second_3d),
                    sum(gmv_uv_second_3d),

                    --近7天
                    sum(expre_cnt_second_7d),
                    sum(expre_uv_second_7d),
                    sum(clk_cnt_second_7d),
                    sum(clk_uv_second_7d),
                    sum(gmv_second_7d),
                    sum(gmv_uv_second_7d),

                    --近14天
                    sum(expre_cnt_second_14d),
                    sum(expre_uv_second_14d),
                    sum(clk_cnt_second_14d),
                    sum(clk_uv_second_14d),
                    sum(gmv_second_14d),
                    sum(gmv_uv_second_14d),

                    --近30天
                    sum(expre_cnt_second_30d),
                    sum(expre_uv_second_30d),
                    sum(clk_cnt_second_30d),
                    sum(clk_uv_second_30d),
                    sum(gmv_second_30d),
                    sum(gmv_uv_second_30d)
from (
         select *
         from tmp.mlb_goods_rate_second_cat_1d
         union all
         select *
         from tmp.mlb_goods_rate_second_cat_3d
         union all
         select *
         from tmp.mlb_goods_rate_second_cat_7d
         union all
         select *
         from tmp.mlb_goods_rate_second_cat_14d
         union all
         select *
         from tmp.mlb_goods_rate_second_cat_30d
     ) t
group by t.goods_id

;




insert overwrite table  tmp.mlb_goods_rate_mct_1d
    select  /*+ REPARTITION(100) */
      a.goods_id,b.add_cart_cnt_mct_1d, collect_cnt_mct_1d, expre_cnt_mct_1d, expre_uv_mct_1d, clk_cnt_mct_1d, clk_uv_mct_1d, gmv_mct_1d, order_cnt_mct_1d, order_uv_mct_1d, add_cart_cnt_mct_3d, collect_cnt_mct_3d, expre_cnt_mct_3d, expre_uv_mct_3d, clk_cnt_mct_3d, clk_uv_mct_3d, gmv_mct_3d, order_cnt_mct_3d, order_uv_mct_3d, add_cart_cnt_mct_7d, collect_cnt_mct_7d, expre_cnt_mct_7d, expre_uv_mct_7d, clk_cnt_mct_7d, clk_uv_mct_7d, gmv_mct_7d, order_cnt_mct_7d, order_uv_mct_7d, add_cart_cnt_mct_14d, collect_cnt_mct_14d, expre_cnt_mct_14d, expre_uv_mct_14d, clk_cnt_mct_14d, clk_uv_mct_14d, gmv_mct_14d, order_cnt_mct_14d, order_uv_mct_14d, add_cart_cnt_mct_30d, collect_cnt_mct_30d, expre_cnt_mct_30d, expre_uv_mct_30d, clk_cnt_mct_30d, clk_uv_mct_30d, gmv_mct_30d, order_cnt_mct_30d, order_uv_mct_30d
    from dim.dim_vova_goods a
    join (
             select b.mct_id,
                    --近1天
                    sum(add_cat_cnt) add_cart_cnt_mct_1d,
                    sum(collect_cnt) collect_cnt_mct_1d,
                    sum(expre_cnt) expre_cnt_mct_1d,
                    count(distinct if(a.expre_cnt > 0,a.buyer_id,null)) expre_uv_mct_1d,
                    sum(clk_cnt)   clk_cnt_mct_1d,
                    count(distinct if(a.clk_cnt > 0,a.buyer_id,null))   clk_uv_mct_1d,
                    sum(gmv)       gmv_mct_1d,
                    sum(a.ord_cnt)   order_cnt_mct_1d,
                    count(distinct if(a.ord_cnt > 0,a.buyer_id,null)) order_uv_mct_1d,

                    --近3天
                    0 add_cart_cnt_mct_3d,
                    0 collect_cnt_mct_3d,
                    0 expre_cnt_mct_3d,
                    0 expre_uv_mct_3d,
                    0 clk_cnt_mct_3d,
                    0 clk_uv_mct_3d,
                    0 gmv_mct_3d,
                    0 order_cnt_mct_3d,
                    0 order_uv_mct_3d,
                    --近7天
                    0 add_cart_cnt_mct_7d,
                    0 collect_cnt_mct_7d,
                    0 expre_cnt_mct_7d,
                    0 expre_uv_mct_7d,
                    0 clk_cnt_mct_7d,
                    0 clk_uv_mct_7d,
                    0 gmv_mct_7d,
                    0 order_cnt_mct_7d,
                    0 order_uv_mct_7d,
                    --近14天
                    0 add_cart_cnt_mct_14d,
                    0 collect_cnt_mct_14d,
                    0 expre_cnt_mct_14d,
                    0 expre_uv_mct_14d,
                    0 clk_cnt_mct_14d,
                    0 clk_uv_mct_14d,
                    0 gmv_mct_14d,
                    0 order_cnt_mct_14d,
                    0 order_uv_mct_14d,
                    --近30天
                    0 add_cart_cnt_mct_30d,
                    0 collect_cnt_mct_30d,
                    0 expre_cnt_mct_30d,
                    0 expre_uv_mct_30d,
                    0 clk_cnt_mct_30d,
                    0 clk_uv_mct_30d,
                    0 gmv_mct_30d,
                    0 order_cnt_mct_30d,
                    0 order_uv_mct_30d

             from dws.dws_vova_buyer_goods_behave a
             join dim.dim_vova_goods b on a.gs_id = b.goods_id
             where a.pt = date_sub('${cur_date}', 1)
             group by b.mct_id
         ) b on a.mct_id = b.mct_id
;



insert overwrite table  tmp.mlb_goods_rate_mct_3d
    select  /*+ REPARTITION(100) */
      a.goods_id,b.add_cart_cnt_mct_1d, collect_cnt_mct_1d, expre_cnt_mct_1d, expre_uv_mct_1d, clk_cnt_mct_1d, clk_uv_mct_1d, gmv_mct_1d, order_cnt_mct_1d, order_uv_mct_1d, add_cart_cnt_mct_3d, collect_cnt_mct_3d, expre_cnt_mct_3d, expre_uv_mct_3d, clk_cnt_mct_3d, clk_uv_mct_3d, gmv_mct_3d, order_cnt_mct_3d, order_uv_mct_3d, add_cart_cnt_mct_7d, collect_cnt_mct_7d, expre_cnt_mct_7d, expre_uv_mct_7d, clk_cnt_mct_7d, clk_uv_mct_7d, gmv_mct_7d, order_cnt_mct_7d, order_uv_mct_7d, add_cart_cnt_mct_14d, collect_cnt_mct_14d, expre_cnt_mct_14d, expre_uv_mct_14d, clk_cnt_mct_14d, clk_uv_mct_14d, gmv_mct_14d, order_cnt_mct_14d, order_uv_mct_14d, add_cart_cnt_mct_30d, collect_cnt_mct_30d, expre_cnt_mct_30d, expre_uv_mct_30d, clk_cnt_mct_30d, clk_uv_mct_30d, gmv_mct_30d, order_cnt_mct_30d, order_uv_mct_30d
    from dim.dim_vova_goods a
    join (
             select b.mct_id,
                    --近1天
                    0 add_cart_cnt_mct_1d,
                    0 collect_cnt_mct_1d,
                    0 expre_cnt_mct_1d,
                    0 expre_uv_mct_1d,
                    0 clk_cnt_mct_1d,
                    0 clk_uv_mct_1d,
                    0 gmv_mct_1d,
                    0 order_cnt_mct_1d,
                    0 order_uv_mct_1d,

                    --近3天
                    sum(add_cat_cnt) add_cart_cnt_mct_3d,
                    sum(collect_cnt) collect_cnt_mct_3d,
                    sum(expre_cnt) expre_cnt_mct_3d,
                    count(distinct if(a.expre_cnt > 0,a.buyer_id,null)) expre_uv_mct_3d,
                    sum(clk_cnt)   clk_cnt_mct_3d,
                    count(distinct if(a.clk_cnt > 0,a.buyer_id,null))   clk_uv_mct_3d,
                    sum(gmv)       gmv_mct_3d,
                    sum(a.ord_cnt)   order_cnt_mct_3d,
                    count(distinct if(a.ord_cnt > 0,a.buyer_id,null)) order_uv_mct_3d,

                    --近7天
                    0 add_cart_cnt_mct_7d,
                    0 collect_cnt_mct_7d,
                    0 expre_cnt_mct_7d,
                    0 expre_uv_mct_7d,
                    0 clk_cnt_mct_7d,
                    0 clk_uv_mct_7d,
                    0 gmv_mct_7d,
                    0 order_cnt_mct_7d,
                    0 order_uv_mct_7d,
                    --近14天
                    0 add_cart_cnt_mct_14d,
                    0 collect_cnt_mct_14d,
                    0 expre_cnt_mct_14d,
                    0 expre_uv_mct_14d,
                    0 clk_cnt_mct_14d,
                    0 clk_uv_mct_14d,
                    0 gmv_mct_14d,
                    0 order_cnt_mct_14d,
                    0 order_uv_mct_14d,
                    --近30天
                    0 add_cart_cnt_mct_30d,
                    0 collect_cnt_mct_30d,
                    0 expre_cnt_mct_30d,
                    0 expre_uv_mct_30d,
                    0 clk_cnt_mct_30d,
                    0 clk_uv_mct_30d,
                    0 gmv_mct_30d,
                    0 order_cnt_mct_30d,
                    0 order_uv_mct_30d

             from dws.dws_vova_buyer_goods_behave a
             join dim.dim_vova_goods b on a.gs_id = b.goods_id
             where a.pt >= date_sub('${cur_date}', 3)
             and a.pt <= date_sub('${cur_date}', 1)
             group by b.mct_id
         ) b on a.mct_id = b.mct_id

;



insert overwrite table   tmp.mlb_goods_rate_mct_7d
    select  /*+ REPARTITION(100) */
      a.goods_id,b.add_cart_cnt_mct_1d, collect_cnt_mct_1d, expre_cnt_mct_1d, expre_uv_mct_1d, clk_cnt_mct_1d, clk_uv_mct_1d, gmv_mct_1d, order_cnt_mct_1d, order_uv_mct_1d, add_cart_cnt_mct_3d, collect_cnt_mct_3d, expre_cnt_mct_3d, expre_uv_mct_3d, clk_cnt_mct_3d, clk_uv_mct_3d, gmv_mct_3d, order_cnt_mct_3d, order_uv_mct_3d, add_cart_cnt_mct_7d, collect_cnt_mct_7d, expre_cnt_mct_7d, expre_uv_mct_7d, clk_cnt_mct_7d, clk_uv_mct_7d, gmv_mct_7d, order_cnt_mct_7d, order_uv_mct_7d, add_cart_cnt_mct_14d, collect_cnt_mct_14d, expre_cnt_mct_14d, expre_uv_mct_14d, clk_cnt_mct_14d, clk_uv_mct_14d, gmv_mct_14d, order_cnt_mct_14d, order_uv_mct_14d, add_cart_cnt_mct_30d, collect_cnt_mct_30d, expre_cnt_mct_30d, expre_uv_mct_30d, clk_cnt_mct_30d, clk_uv_mct_30d, gmv_mct_30d, order_cnt_mct_30d, order_uv_mct_30d
    from dim.dim_vova_goods a
    join (
             select b.mct_id,
                    --近1天
                    0 add_cart_cnt_mct_1d,
                    0 collect_cnt_mct_1d,
                    0 expre_cnt_mct_1d,
                    0 expre_uv_mct_1d,
                    0 clk_cnt_mct_1d,
                    0 clk_uv_mct_1d,
                    0 gmv_mct_1d,
                    0 order_cnt_mct_1d,
                    0 order_uv_mct_1d,

                    --近3天
                    0 add_cart_cnt_mct_3d,
                    0 collect_cnt_mct_3d,
                    0 expre_cnt_mct_3d,
                    0 expre_uv_mct_3d,
                    0 clk_cnt_mct_3d,
                    0 clk_uv_mct_3d,
                    0 gmv_mct_3d,
                    0 order_cnt_mct_3d,
                    0 order_uv_mct_3d,


                    --近7天
                    sum(add_cat_cnt) add_cart_cnt_mct_7d,
                    sum(collect_cnt) collect_cnt_mct_7d,
                    sum(expre_cnt) expre_cnt_mct_7d,
                    count(distinct if(a.expre_cnt > 0,a.buyer_id,null)) expre_uv_mct_7d,
                    sum(clk_cnt)   clk_cnt_mct_7d,
                    count(distinct if(a.clk_cnt > 0,a.buyer_id,null))   clk_uv_mct_7d,
                    sum(gmv)       gmv_mct_7d,
                    sum(a.ord_cnt)   order_cnt_mct_7d,
                    count(distinct if(a.ord_cnt > 0,a.buyer_id,null)) order_uv_mct_7d,
                    --近14天
                    0 add_cart_cnt_mct_14d,
                    0 collect_cnt_mct_14d,
                    0 expre_cnt_mct_14d,
                    0 expre_uv_mct_14d,
                    0 clk_cnt_mct_14d,
                    0 clk_uv_mct_14d,
                    0 gmv_mct_14d,
                    0 order_cnt_mct_14d,
                    0 order_uv_mct_14d,
                    --近30天
                    0 add_cart_cnt_mct_30d,
                    0 collect_cnt_mct_30d,
                    0 expre_cnt_mct_30d,
                    0 expre_uv_mct_30d,
                    0 clk_cnt_mct_30d,
                    0 clk_uv_mct_30d,
                    0 gmv_mct_30d,
                    0 order_cnt_mct_30d,
                    0 order_uv_mct_30d

             from dws.dws_vova_buyer_goods_behave a
             join dim.dim_vova_goods b on a.gs_id = b.goods_id
             where a.pt >= date_sub('${cur_date}', 7)
             and a.pt <= date_sub('${cur_date}', 1)
             group by b.mct_id
         ) b on a.mct_id = b.mct_id
;





insert overwrite table   tmp.mlb_goods_rate_mct_14d
    select  /*+ REPARTITION(100) */
      a.goods_id,b.add_cart_cnt_mct_1d, collect_cnt_mct_1d, expre_cnt_mct_1d, expre_uv_mct_1d, clk_cnt_mct_1d, clk_uv_mct_1d, gmv_mct_1d, order_cnt_mct_1d, order_uv_mct_1d, add_cart_cnt_mct_3d, collect_cnt_mct_3d, expre_cnt_mct_3d, expre_uv_mct_3d, clk_cnt_mct_3d, clk_uv_mct_3d, gmv_mct_3d, order_cnt_mct_3d, order_uv_mct_3d, add_cart_cnt_mct_7d, collect_cnt_mct_7d, expre_cnt_mct_7d, expre_uv_mct_7d, clk_cnt_mct_7d, clk_uv_mct_7d, gmv_mct_7d, order_cnt_mct_7d, order_uv_mct_7d, add_cart_cnt_mct_14d, collect_cnt_mct_14d, expre_cnt_mct_14d, expre_uv_mct_14d, clk_cnt_mct_14d, clk_uv_mct_14d, gmv_mct_14d, order_cnt_mct_14d, order_uv_mct_14d, add_cart_cnt_mct_30d, collect_cnt_mct_30d, expre_cnt_mct_30d, expre_uv_mct_30d, clk_cnt_mct_30d, clk_uv_mct_30d, gmv_mct_30d, order_cnt_mct_30d, order_uv_mct_30d
    from dim.dim_vova_goods a
    join (
             select b.mct_id,
                    --近1天
                    0 add_cart_cnt_mct_1d,
                    0 collect_cnt_mct_1d,
                    0 expre_cnt_mct_1d,
                    0 expre_uv_mct_1d,
                    0 clk_cnt_mct_1d,
                    0 clk_uv_mct_1d,
                    0 gmv_mct_1d,
                    0 order_cnt_mct_1d,
                    0 order_uv_mct_1d,

                    --近3天
                    0 add_cart_cnt_mct_3d,
                    0 collect_cnt_mct_3d,
                    0 expre_cnt_mct_3d,
                    0 expre_uv_mct_3d,
                    0 clk_cnt_mct_3d,
                    0 clk_uv_mct_3d,
                    0 gmv_mct_3d,
                    0 order_cnt_mct_3d,
                    0 order_uv_mct_3d,


                    --近7天
                    0 add_cart_cnt_mct_7d,
                    0 collect_cnt_mct_7d,
                    0 expre_cnt_mct_7d,
                    0 expre_uv_mct_7d,
                    0 clk_cnt_mct_7d,
                    0 clk_uv_mct_7d,
                    0 gmv_mct_7d,
                    0 order_cnt_mct_7d,
                    0 order_uv_mct_7d,

                    --近14天
                    sum(add_cat_cnt) add_cart_cnt_mct_14d,
                    sum(collect_cnt) collect_cnt_mct_14d,
                    sum(expre_cnt) expre_cnt_mct_14d,
                    count(distinct if(a.expre_cnt > 0,a.buyer_id,null)) expre_uv_mct_14d,
                    sum(clk_cnt)   clk_cnt_mct_14d,
                    count(distinct if(a.clk_cnt > 0,a.buyer_id,null))   clk_uv_mct_14d,
                    sum(gmv)       gmv_mct_14d,
                    sum(a.ord_cnt)   order_cnt_mct_14d,
                    count(distinct if(a.ord_cnt > 0,a.buyer_id,null)) order_uv_mct_14d,
                    --近30天
                    0 add_cart_cnt_mct_30d,
                    0 collect_cnt_mct_30d,
                    0 expre_cnt_mct_30d,
                    0 expre_uv_mct_30d,
                    0 clk_cnt_mct_30d,
                    0 clk_uv_mct_30d,
                    0 gmv_mct_30d,
                    0 order_cnt_mct_30d,
                    0 order_uv_mct_30d

             from dws.dws_vova_buyer_goods_behave a
             join dim.dim_vova_goods b on a.gs_id = b.goods_id
             where a.pt >= date_sub('${cur_date}', 14)
             and a.pt <= date_sub('${cur_date}', 1)
             group by b.mct_id
         ) b on a.mct_id = b.mct_id
;


insert overwrite table  tmp.mlb_goods_rate_mct_30d
    select  /*+ REPARTITION(100) */
      a.goods_id,b.add_cart_cnt_mct_1d, collect_cnt_mct_1d, expre_cnt_mct_1d, expre_uv_mct_1d, clk_cnt_mct_1d, clk_uv_mct_1d, gmv_mct_1d, order_cnt_mct_1d, order_uv_mct_1d, add_cart_cnt_mct_3d, collect_cnt_mct_3d, expre_cnt_mct_3d, expre_uv_mct_3d, clk_cnt_mct_3d, clk_uv_mct_3d, gmv_mct_3d, order_cnt_mct_3d, order_uv_mct_3d, add_cart_cnt_mct_7d, collect_cnt_mct_7d, expre_cnt_mct_7d, expre_uv_mct_7d, clk_cnt_mct_7d, clk_uv_mct_7d, gmv_mct_7d, order_cnt_mct_7d, order_uv_mct_7d, add_cart_cnt_mct_14d, collect_cnt_mct_14d, expre_cnt_mct_14d, expre_uv_mct_14d, clk_cnt_mct_14d, clk_uv_mct_14d, gmv_mct_14d, order_cnt_mct_14d, order_uv_mct_14d, add_cart_cnt_mct_30d, collect_cnt_mct_30d, expre_cnt_mct_30d, expre_uv_mct_30d, clk_cnt_mct_30d, clk_uv_mct_30d, gmv_mct_30d, order_cnt_mct_30d, order_uv_mct_30d
    from dim.dim_vova_goods a
    join (
             select b.mct_id,
                    --近1天
                    0 add_cart_cnt_mct_1d,
                    0 collect_cnt_mct_1d,
                    0 expre_cnt_mct_1d,
                    0 expre_uv_mct_1d,
                    0 clk_cnt_mct_1d,
                    0 clk_uv_mct_1d,
                    0 gmv_mct_1d,
                    0 order_cnt_mct_1d,
                    0 order_uv_mct_1d,

                    --近3天
                    0 add_cart_cnt_mct_3d,
                    0 collect_cnt_mct_3d,
                    0 expre_cnt_mct_3d,
                    0 expre_uv_mct_3d,
                    0 clk_cnt_mct_3d,
                    0 clk_uv_mct_3d,
                    0 gmv_mct_3d,
                    0 order_cnt_mct_3d,
                    0 order_uv_mct_3d,


                    --近7天
                    0 add_cart_cnt_mct_7d,
                    0 collect_cnt_mct_7d,
                    0 expre_cnt_mct_7d,
                    0 expre_uv_mct_7d,
                    0 clk_cnt_mct_7d,
                    0 clk_uv_mct_7d,
                    0 gmv_mct_7d,
                    0 order_cnt_mct_7d,
                    0 order_uv_mct_7d,

                    --近14天
                    0 add_cart_cnt_mct_14d,
                    0 collect_cnt_mct_14d,
                    0 expre_cnt_mct_14d,
                    0 expre_uv_mct_14d,
                    0 clk_cnt_mct_14d,
                    0 clk_uv_mct_14d,
                    0 gmv_mct_14d,
                    0 order_cnt_mct_14d,
                    0 order_uv_mct_14d,
                    --近30天
                    sum(add_cat_cnt) add_cart_cnt_mct_30d,
                    sum(collect_cnt) collect_cnt_mct_30d,
                    sum(expre_cnt) expre_cnt_mct_30d,
                    count(distinct if(a.expre_cnt > 0,a.buyer_id,null)) expre_uv_mct_30d,
                    sum(clk_cnt)   clk_cnt_mct_30d,
                    count(distinct if(a.clk_cnt > 0,a.buyer_id,null))   clk_uv_mct_30d,
                    sum(gmv)       gmv_mct_30d,
                    sum(a.ord_cnt)   order_cnt_mct_30d,
                    count(distinct if(a.ord_cnt > 0,a.buyer_id,null)) order_uv_mct_30d

             from dws.dws_vova_buyer_goods_behave a
             join dim.dim_vova_goods b on a.gs_id = b.goods_id
             where a.pt >= date_sub('${cur_date}', 30)
             and a.pt <= date_sub('${cur_date}', 1)
             group by b.mct_id
         ) b on a.mct_id = b.mct_id
;


insert overwrite table tmp.mlb_goods_rate_mct
select  /*+ REPARTITION(100) */
t.goods_id,
                    sum(add_cart_cnt_mct_1d),
                    sum(collect_cnt_mct_1d),
                    sum(expre_cnt_mct_1d),
                    sum(expre_uv_mct_1d),
                    sum(clk_cnt_mct_1d),
                    sum(clk_uv_mct_1d),
                    sum(gmv_mct_1d),
                    sum(order_cnt_mct_1d),
                    sum(order_uv_mct_1d),

                     --近3天
                    sum(add_cart_cnt_mct_3d),
                    sum(collect_cnt_mct_3d),
                    sum(expre_cnt_mct_3d),
                    sum(expre_uv_mct_3d),
                    sum(clk_cnt_mct_3d),
                    sum(clk_uv_mct_3d),
                    sum(gmv_mct_3d),
                    sum(order_cnt_mct_3d),
                    sum(order_uv_mct_3d),


                     --近7天
                    sum(add_cart_cnt_mct_7d),
                    sum(collect_cnt_mct_7d),
                    sum(expre_cnt_mct_7d),
                    sum(expre_uv_mct_7d),
                    sum(clk_cnt_mct_7d),
                    sum(clk_uv_mct_7d),
                    sum(gmv_mct_7d),
                    sum(order_cnt_mct_7d),
                    sum(order_uv_mct_7d),

                     --近14天
                    sum(add_cart_cnt_mct_14d),
                    sum(collect_cnt_mct_14d),
                    sum(expre_cnt_mct_14d),
                    sum(expre_uv_mct_14d),
                    sum(clk_cnt_mct_14d),
                    sum(clk_uv_mct_14d),
                    sum(gmv_mct_14d),
                    sum(order_cnt_mct_14d),
                    sum(order_uv_mct_14d),

                       --近30天
                    sum(add_cart_cnt_mct_30d),
                    sum(collect_cnt_mct_30d),
                    sum(expre_cnt_mct_30d),
                    sum(expre_uv_mct_30d),
                    sum(clk_cnt_mct_30d),
                    sum(clk_uv_mct_30d),
                    sum(gmv_mct_30d),
                    sum(order_cnt_mct_30d),
                    sum(order_uv_mct_30d)

from (
         select *
         from tmp.mlb_goods_rate_mct_1d
         union all
         select *
         from tmp.mlb_goods_rate_mct_3d
         union all
         select *
         from tmp.mlb_goods_rate_mct_7d
         union all
         select *
         from tmp.mlb_goods_rate_mct_14d
         union all
         select *
         from tmp.mlb_goods_rate_mct_30d
     ) t
group by t.goods_id
;



INSERT OVERWRITE TABLE mlb.mlb_vova_goods_rate partition (pt = '${cur_date}')
select
a.goods_id,
datediff('${cur_date}',to_date(a.last_on_time)) onsale_days,
a.first_cat_id,
a.second_cat_id,
a.third_cat_id,
a.fourth_cat_id,
a.cat_id,
a.shop_price,
if(a.brand_id > 0, 1, 0) is_brand,
round(b.entry_warehouse_72h_order_goods / b.collection_order_goods,4) gather_rate,
b.inter_rate_3_6w,
b.nlrf_rate_5_8w,
b.lrf_rate_9_12w,
c.score mct_score,
c.rank mct_rank,
b.comment_cnt_6m,
b.comment_good_cnt_6m,
b.comment_bad_cnt_6m,
round(b.comment_good_cnt_6m / b.comment_cnt_6m,4) good_comment_rate_6m,
round(b.comment_bad_cnt_6m / b.comment_cnt_6m,4) bad_comment_rate_6m,
--商品最近1天特征
d.expre_cnt_1d,
d.expre_uv_1d,
d.clk_cnt_1d,
d.clk_uv_1d,
d.clk_rate_1d,
d.cr_rate_1d,
d.expre_cnt_per_u_1d,
d.clk_cnt_per_u_1d,
d.gmv_1d,
d.collect_cnt_1d,
d.collect_uv_1d,
d.add_cart_cnt_1d,
d.add_cart_uv_1d,
d.order_cnt_1d,
d.order_uv_1d,
d.sale_vol_1d,
e.comment_cnt_1d,
e.good_comment_cnt_1d,
e.bad_comment_cnt_1d,
f.avg_stay_time_1d,
g.refund_order_cnt_1d,
g.refund_amt_1d,
round(d.expre_cnt_1d / h.expre_cnt_second_1d,4) expre_on_second_rate_1d,
round(d.clk_cnt_1d / h.clk_cnt_second_1d,4) clk_on_second_rate_1d,
round(d.gmv_1d / h.gmv_second_1d,4) gmv_on_second_rate_1d,
round(d.expre_cnt_1d / i.expre_cnt_1d,4) expre_on_brand_rate_1d,
round(d.clk_cnt_1d / i.clk_cnt_1d,4) clk_on_brand_rate_1d,
round(d.gmv_1d / i.gmv_1d,4) gmv_on_brand_rate_1d,
round(d.gmv_1d / d.expre_cnt_1d,4) expre_effciency_1d,
round(d.gmv_1d / (d.clk_uv_1d + 5),4) gr_1d,
round(d.add_cart_cnt_1d / j.add_cart_cnt_mct_1d,4) add_cart_on_mct_rate_1d,
round(d.collect_cnt_1d / j.collect_cnt_mct_1d,4) collect_on_mct_rate_1d,
round(d.expre_cnt_1d / j.expre_cnt_mct_1d,4) expre_on_mct_rate_1d,
round(d.clk_cnt_1d / j.clk_cnt_mct_1d,4) clk_on_mct_rate_1d,
round(d.gmv_1d / j.gmv_mct_1d,4) gmv_on_mct_rate_1d,


--商品最近3天特征
d.expre_cnt_3d,
d.expre_uv_3d,
d.clk_cnt_3d,
d.clk_uv_3d,
d.clk_rate_3d,
d.cr_rate_3d,
d.expre_cnt_per_u_3d,
d.clk_cnt_per_u_3d,
d.gmv_3d,
d.collect_cnt_3d,
d.collect_uv_3d,
d.add_cart_cnt_3d,
d.add_cart_uv_3d,
d.order_cnt_3d,
d.order_uv_3d,
d.sale_vol_3d,
e.comment_cnt_3d,
e.good_comment_cnt_3d,
e.bad_comment_cnt_3d,
f.avg_stay_time_3d,
g.refund_order_cnt_3d,
g.refund_amt_3d,
round(d.expre_cnt_3d / h.expre_cnt_second_3d,4) expre_on_second_rate_3d,
round(d.clk_cnt_3d / h.clk_cnt_second_3d,4) clk_on_second_rate_3d,
round(d.gmv_3d / h.gmv_second_3d,4) gmv_on_second_rate_3d,
round(d.expre_cnt_3d / i.expre_cnt_3d,4) expre_on_brand_rate_3d,
round(d.clk_cnt_3d / i.clk_cnt_3d,4) clk_on_brand_rate_3d,
round(d.gmv_3d / i.gmv_3d,4) gmv_on_brand_rate_3d,
round(d.gmv_3d / d.expre_cnt_3d,4) expre_effciency_3d,
round(d.gmv_3d / (d.clk_uv_3d + 5),4) gr_3d,
round(d.add_cart_cnt_3d / j.add_cart_cnt_mct_3d,4) add_cart_on_mct_rate_3d,
round(d.collect_cnt_3d / j.collect_cnt_mct_3d,4) collect_on_mct_rate_3d,
round(d.expre_cnt_3d / j.expre_cnt_mct_3d,4) expre_on_mct_rate_3d,
round(d.clk_cnt_3d / j.clk_cnt_mct_3d,4) clk_on_mct_rate_3d,
round(d.gmv_3d / j.gmv_mct_3d,4) gmv_on_mct_rate_3d,


--商品最近7天特征
d.expre_cnt_7d,
d.expre_uv_7d,
d.clk_cnt_7d,
d.clk_uv_7d,
d.clk_rate_7d,
d.cr_rate_7d,
d.expre_cnt_per_u_7d,
d.clk_cnt_per_u_7d,
d.gmv_7d,
d.collect_cnt_7d,
d.collect_uv_7d,
d.add_cart_cnt_7d,
d.add_cart_uv_7d,
d.order_cnt_7d,
d.order_uv_7d,
d.sale_vol_7d,
e.comment_cnt_7d,
e.good_comment_cnt_7d,
e.bad_comment_cnt_7d,
f.avg_stay_time_7d,
g.refund_order_cnt_7d,
g.refund_amt_7d,
round(d.expre_cnt_7d / h.expre_cnt_second_7d,4) expre_on_second_rate_7d,
round(d.clk_cnt_7d / h.clk_cnt_second_7d,4) clk_on_second_rate_7d,
round(d.gmv_7d / h.gmv_second_7d,4) gmv_on_second_rate_7d,
round(d.expre_cnt_7d / i.expre_cnt_7d,4) expre_on_brand_rate_7d,
round(d.clk_cnt_7d / i.clk_cnt_7d,4) clk_on_brand_rate_7d,
round(d.gmv_7d / i.gmv_7d,4) gmv_on_brand_rate_7d,
round(d.gmv_7d / d.expre_cnt_7d,4) expre_effciency_7d,
round(d.gmv_7d / (d.clk_uv_7d + 5),4) gr_7d,
round(d.add_cart_cnt_7d / j.add_cart_cnt_mct_7d,4) add_cart_on_mct_rate_7d,
round(d.collect_cnt_7d / j.collect_cnt_mct_7d,4) collect_on_mct_rate_7d,
round(d.expre_cnt_7d / j.expre_cnt_mct_7d,4) expre_on_mct_rate_7d,
round(d.clk_cnt_7d / j.clk_cnt_mct_7d,4) clk_on_mct_rate_7d,
round(d.gmv_7d / j.gmv_mct_7d,4) gmv_on_mct_rate_7d,



--商品最近14天特征
d.expre_cnt_14d,
d.expre_uv_14d,
d.clk_cnt_14d,
d.clk_uv_14d,
d.clk_rate_14d,
d.cr_rate_14d,
d.expre_cnt_per_u_14d,
d.clk_cnt_per_u_14d,
d.gmv_14d,
d.collect_cnt_14d,
d.collect_uv_14d,
d.add_cart_cnt_14d,
d.add_cart_uv_14d,
d.order_cnt_14d,
d.order_uv_14d,
d.sale_vol_14d,
e.comment_cnt_14d,
e.good_comment_cnt_14d,
e.bad_comment_cnt_14d,
f.avg_stay_time_14d,
g.refund_order_cnt_14d,
g.refund_amt_14d,
round(d.expre_cnt_14d / h.expre_cnt_second_14d,4) expre_on_second_rate_14d,
round(d.clk_cnt_14d / h.clk_cnt_second_14d,4) clk_on_second_rate_14d,
round(d.gmv_14d / h.gmv_second_14d,4) gmv_on_second_rate_14d,
round(d.expre_cnt_14d / i.expre_cnt_14d,4) expre_on_brand_rate_14d,
round(d.clk_cnt_14d / i.clk_cnt_14d,4) clk_on_brand_rate_14d,
round(d.gmv_14d / i.gmv_14d,4) gmv_on_brand_rate_14d,
round(d.gmv_14d / d.expre_cnt_14d,4) expre_effciency_14d,
round(d.gmv_14d / (d.clk_uv_14d + 5),4) gr_14d,
round(d.add_cart_cnt_14d / j.add_cart_cnt_mct_14d,4) add_cart_on_mct_rate_14d,
round(d.collect_cnt_14d / j.collect_cnt_mct_14d,4) collect_on_mct_rate_14d,
round(d.expre_cnt_14d / j.expre_cnt_mct_14d,4) expre_on_mct_rate_14d,
round(d.clk_cnt_14d / j.clk_cnt_mct_14d,4) clk_on_mct_rate_14d,
round(d.gmv_14d / j.gmv_mct_14d,4) gmv_on_mct_rate_14d,



--商品最近30天特征
d.expre_cnt_30d,
d.expre_uv_30d,
d.clk_cnt_30d,
d.clk_uv_30d,
d.clk_rate_30d,
d.cr_rate_30d,
d.expre_cnt_per_u_30d,
d.clk_cnt_per_u_30d,
d.gmv_30d,
d.collect_cnt_30d,
d.collect_uv_30d,
d.add_cart_cnt_30d,
d.add_cart_uv_30d,
d.order_cnt_30d,
d.order_uv_30d,
d.sale_vol_30d,
e.comment_cnt_30d,
e.good_comment_cnt_30d,
e.bad_comment_cnt_30d,
f.avg_stay_time_30d,
g.refund_order_cnt_30d,
g.refund_amt_30d,
round(d.expre_cnt_30d / h.expre_cnt_second_30d,4) expre_on_second_rate_30d,
round(d.clk_cnt_30d / h.clk_cnt_second_30d,4) clk_on_second_rate_30d,
round(d.gmv_30d / h.gmv_second_30d,4) gmv_on_second_rate_30d,
round(d.expre_cnt_30d / i.expre_cnt_30d,4) expre_on_brand_rate_30d,
round(d.clk_cnt_30d / i.clk_cnt_30d,4) clk_on_brand_rate_30d,
round(d.gmv_30d / i.gmv_30d,4) gmv_on_brand_rate_30d,
round(d.gmv_30d / d.expre_cnt_30d,4) expre_effciency_30d,
round(d.gmv_30d / (d.clk_uv_30d + 5),4) gr_30d,
round(d.add_cart_cnt_30d / j.add_cart_cnt_mct_30d,4) add_cart_on_mct_rate_30d,
round(d.collect_cnt_30d / j.collect_cnt_mct_30d,4) collect_on_mct_rate_30d,
round(d.expre_cnt_30d / j.expre_cnt_mct_30d,4) expre_on_mct_rate_30d,
round(d.clk_cnt_30d / j.clk_cnt_mct_30d,4) clk_on_mct_rate_30d,
round(d.gmv_30d / j.gmv_mct_30d,4) gmv_on_mct_rate_30d,

--商品所在店铺近1天特征
j.expre_cnt_mct_1d,
j.clk_cnt_mct_1d,
round(j.clk_cnt_mct_1d / j.expre_cnt_mct_1d,4) clk_rate_mct_1d,
j.expre_uv_mct_1d,
j.clk_uv_mct_1d,
j.add_cart_cnt_mct_1d,
j.collect_cnt_mct_1d,
j.order_cnt_mct_1d,
round(j.order_uv_mct_1d / j.expre_uv_mct_1d,4) cr_rate_mct_1d,
j.gmv_mct_1d,
k.avg_stay_time_mct_1d,

--商品所在店铺近3天特征
j.expre_cnt_mct_3d,
j.clk_cnt_mct_3d,
round(j.clk_cnt_mct_3d / j.expre_cnt_mct_3d,4) clk_rate_mct_3d,
j.expre_uv_mct_3d,
j.clk_uv_mct_3d,
j.add_cart_cnt_mct_3d,
j.collect_cnt_mct_3d,
j.order_cnt_mct_3d,
round(j.order_uv_mct_3d / j.expre_uv_mct_3d,4) cr_rate_mct_3d,
j.gmv_mct_3d,
k.avg_stay_time_mct_3d,

--商品所在店铺近7天特征
j.expre_cnt_mct_7d,
j.clk_cnt_mct_7d,
round(j.clk_cnt_mct_7d / j.expre_cnt_mct_7d,4) clk_rate_mct_7d,
j.expre_uv_mct_7d,
j.clk_uv_mct_7d,
j.add_cart_cnt_mct_7d,
j.collect_cnt_mct_7d,
j.order_cnt_mct_7d,
round(j.order_uv_mct_7d / j.expre_uv_mct_7d,4) cr_rate_mct_7d,
j.gmv_mct_7d,
k.avg_stay_time_mct_7d,

--商品所在店铺近14天特征
j.expre_cnt_mct_14d,
j.clk_cnt_mct_14d,
round(j.clk_cnt_mct_14d / j.expre_cnt_mct_14d,4) clk_rate_mct_14d,
j.expre_uv_mct_14d,
j.clk_uv_mct_14d,
j.add_cart_cnt_mct_14d,
j.collect_cnt_mct_14d,
j.order_cnt_mct_14d,
round(j.order_uv_mct_14d / j.expre_uv_mct_14d,4) cr_rate_mct_14d,
j.gmv_mct_14d,
k.avg_stay_time_mct_14d,

--商品所在店铺近30天特征
j.expre_cnt_mct_30d,
j.clk_cnt_mct_30d,
round(j.clk_cnt_mct_30d / j.expre_cnt_mct_30d,4) clk_rate_mct_30d,
j.expre_uv_mct_30d,
j.clk_uv_mct_30d,
j.add_cart_cnt_mct_30d,
j.collect_cnt_mct_30d,
j.order_cnt_mct_30d,
round(j.order_uv_mct_30d / j.expre_uv_mct_30d,4) cr_rate_mct_30d,
j.gmv_mct_30d,
k.avg_stay_time_mct_30d,

--商品所在二级类目近1天的特征
h.expre_cnt_second_1d,
h.clk_cnt_second_1d,
round(h.clk_cnt_second_1d / h.expre_cnt_second_1d,4) clk_rate_second_1d,
h.expre_uv_second_1d,
h.clk_uv_second_1d,
round(h.gmv_uv_second_1d / h.expre_uv_second_1d,4) cr_rate_second_1d,
h.gmv_second_1d,

--商品所在二级类目近3天的特征
h.expre_cnt_second_3d,
h.clk_cnt_second_3d,
round(h.clk_cnt_second_3d / h.expre_cnt_second_3d,4) clk_rate_second_3d,
h.expre_uv_second_3d,
h.clk_uv_second_3d,
round(h.gmv_uv_second_3d / h.expre_uv_second_3d,4) cr_rate_second_3d,
h.gmv_second_3d,

--商品所在二级类目近7天的特征
h.expre_cnt_second_7d,
h.clk_cnt_second_7d,
round(h.clk_cnt_second_7d / h.expre_cnt_second_7d,4) clk_rate_second_7d,
h.expre_uv_second_7d,
h.clk_uv_second_7d,
round(h.gmv_uv_second_7d / h.expre_uv_second_7d,4) cr_rate_second_7d,
h.gmv_second_7d,

--商品所在二级类目近14天的特征
h.expre_cnt_second_14d,
h.clk_cnt_second_14d,
round(h.clk_cnt_second_14d / h.expre_cnt_second_14d,4) clk_rate_second_14d,
h.expre_uv_second_14d,
h.clk_uv_second_14d,
round(h.gmv_uv_second_14d / h.expre_uv_second_14d,4) cr_rate_second_14d,
h.gmv_second_14d,

--商品所在二级类目近30天的特征
h.expre_cnt_second_30d,
h.clk_cnt_second_30d,
round(h.clk_cnt_second_30d / h.expre_cnt_second_30d,4) clk_rate_second_30d,
h.expre_uv_second_30d,
h.clk_uv_second_30d,
round(h.gmv_uv_second_30d / h.expre_uv_second_30d,4) cr_rate_second_30d,
h.gmv_second_30d,

--商品今日标签
if(l.gmv > 0,1,0) is_order,
l.ord_cnt,
l.sales_vol,
if(l.add_cat_cnt > 0,1,0) is_add_cat,
l.add_cat_cnt,
b.is_recommend

from dim.dim_vova_goods a
left join (select * from ads.ads_vova_goods_portrait where pt = '${cur_date}') b on a.goods_id = b.goods_id
left join (select * from ads.ads_vova_mct_rank where pt = '${cur_date}') c on a.first_cat_id = c.first_cat_id and a.mct_id = c.mct_id
join tmp.mlb_goods_rate_01 d on a.goods_id = d.gs_id
left join (
      SELECT
      goods_id,
      --近1天
      sum( if(to_date ( post_time ) = date_sub('${cur_date}',1),1,0) ) AS comment_cnt_1d,
      sum( IF ( rating <= 2 and to_date ( post_time ) = date_sub('${cur_date}',1), 1, 0 ) ) AS bad_comment_cnt_1d,
      sum( IF ( rating = 5 and to_date ( post_time ) = date_sub('${cur_date}',1), 1, 0 ) ) AS good_comment_cnt_1d,

       --近3天
      sum( if(to_date ( post_time ) >= date_sub('${cur_date}',3) and to_date ( post_time ) <= date_sub('${cur_date}',1),1,0) ) AS comment_cnt_3d,
      sum( IF ( rating <= 2 and to_date ( post_time ) >= date_sub('${cur_date}',3) and to_date ( post_time ) <= date_sub('${cur_date}',1), 1, 0 ) ) AS bad_comment_cnt_3d,
      sum( IF ( rating = 5 and to_date ( post_time ) >= date_sub('${cur_date}',3) and to_date ( post_time ) <= date_sub('${cur_date}',1), 1, 0 ) ) AS good_comment_cnt_3d,

       --近7天
      sum( if(to_date ( post_time ) >= date_sub('${cur_date}',7) and to_date ( post_time ) <= date_sub('${cur_date}',1),1,0) ) AS comment_cnt_7d,
      sum( IF ( rating <= 2 and to_date ( post_time ) >= date_sub('${cur_date}',7) and to_date ( post_time ) <= date_sub('${cur_date}',1), 1, 0 ) ) AS bad_comment_cnt_7d,
      sum( IF ( rating = 5 and to_date ( post_time ) >= date_sub('${cur_date}',7) and to_date ( post_time ) <= date_sub('${cur_date}',1), 1, 0 ) ) AS good_comment_cnt_7d,

       --近14天
      sum( if(to_date ( post_time ) >= date_sub('${cur_date}',14) and to_date ( post_time ) <= date_sub('${cur_date}',1),1,0) ) AS comment_cnt_14d,
      sum( IF ( rating <= 2 and to_date ( post_time ) >= date_sub('${cur_date}',14) and to_date ( post_time ) <= date_sub('${cur_date}',1), 1, 0 ) ) AS bad_comment_cnt_14d,
      sum( IF ( rating = 5 and to_date ( post_time ) >= date_sub('${cur_date}',14) and to_date ( post_time ) <= date_sub('${cur_date}',1), 1, 0 ) ) AS good_comment_cnt_14d,

       --近30天
      sum( if(to_date ( post_time ) >= date_sub('${cur_date}',30) and to_date ( post_time ) <= date_sub('${cur_date}',1),1,0) ) AS comment_cnt_30d,
      sum( IF ( rating <= 2 and to_date ( post_time ) >= date_sub('${cur_date}',30) and to_date ( post_time ) <= date_sub('${cur_date}',1), 1, 0 ) ) AS bad_comment_cnt_30d,
      sum( IF ( rating = 5 and to_date ( post_time ) >= date_sub('${cur_date}',30) and to_date ( post_time ) <= date_sub('${cur_date}',1), 1, 0 ) ) AS good_comment_cnt_30d

    FROM
      dwd.dwd_vova_fact_comment
    WHERE
      to_date ( post_time ) > date_sub( '${cur_date}', 30 )
      AND to_date ( post_time ) <= date_sub('${cur_date}',1)
    GROUP BY
      goods_id
    ) e on a.goods_id = e.goods_id

left join (
    select
    b.goods_id,
    --近1天
    sum(if(a.pt = date_sub('${cur_date}',1) and a.leave_ts != 0 and  a.enter_ts != 0,if(a.leave_ts - a.enter_ts > 300 * 1000 ,300 * 1000,a.leave_ts - a.enter_ts),0)) / sum(if(a.pt = date_sub('${cur_date}',1) and a.leave_ts != 0 and a.enter_ts != 0,1,0)) / 1000 avg_stay_time_1d,

   --近3天
    sum(if(a.pt >= date_sub('${cur_date}',3) and a.pt <= date_sub('${cur_date}',1) and a.leave_ts != 0 and  a.enter_ts != 0,if(a.leave_ts - a.enter_ts > 300 * 1000 ,300 * 1000,a.leave_ts - a.enter_ts),0)) / sum(if(a.pt >= date_sub('${cur_date}',3) and a.pt <= date_sub('${cur_date}',1) and a.leave_ts != 0 and a.enter_ts != 0,1,0)) / 1000 avg_stay_time_3d,

   --近7天
    sum(if(a.pt >= date_sub('${cur_date}',7) and a.pt <= date_sub('${cur_date}',1) and a.leave_ts != 0 and  a.enter_ts != 0,if(a.leave_ts - a.enter_ts > 300 * 1000 ,300 * 1000,a.leave_ts - a.enter_ts),0)) / sum(if(a.pt >= date_sub('${cur_date}',7) and a.pt <= date_sub('${cur_date}',1) and a.leave_ts != 0 and a.enter_ts != 0,1,0)) / 1000 avg_stay_time_7d,

   --近14天
    sum(if(a.pt >= date_sub('${cur_date}',14) and a.pt <= date_sub('${cur_date}',1) and a.leave_ts != 0 and  a.enter_ts != 0,if(a.leave_ts - a.enter_ts > 300 * 1000 ,300 * 1000,a.leave_ts - a.enter_ts),0)) / sum(if(a.pt >= date_sub('${cur_date}',14) and a.pt <= date_sub('${cur_date}',1) and a.leave_ts != 0 and a.enter_ts != 0,1,0)) / 1000 avg_stay_time_14d,

   --近30天
    sum(if(a.pt >= date_sub('${cur_date}',30) and a.pt <= date_sub('${cur_date}',1) and a.leave_ts != 0 and  a.enter_ts != 0,if(a.leave_ts - a.enter_ts > 300 * 1000 ,300 * 1000,a.leave_ts - a.enter_ts),0)) / sum(if(a.pt >= date_sub('${cur_date}',30) and a.pt <= date_sub('${cur_date}',1) and a.leave_ts != 0 and a.enter_ts != 0,1,0)) / 1000 avg_stay_time_30d

from dwd.dwd_vova_log_page_view_arc a
join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
where a.pt >= date_sub('${cur_date}',30) and a.pt <= date_sub('${cur_date}',1)
and page_code = 'product_detail'
group by b.goods_id
    ) f on a.goods_id = f.goods_id

left join tmp.mlb_goods_rate_refund g on a.goods_id = g.goods_id
left join tmp.mlb_goods_rate_second_cat h on a.goods_id = h.goods_id
left join (
        select
          a.goods_id,b.expre_cnt_1d, clk_cnt_1d, gmv_1d, expre_cnt_3d, clk_cnt_3d, gmv_3d, expre_cnt_7d, clk_cnt_7d, gmv_7d, expre_cnt_14d, clk_cnt_14d, gmv_14d, expre_cnt_30d, clk_cnt_30d, gmv_30d
        from dim.dim_vova_goods a
        join (
                 select brand_id,
                        --近1天
                        sum(if(a.pt = date_sub('${cur_date}',1),expre_cnt,0)) expre_cnt_1d,
                        sum(if(a.pt = date_sub('${cur_date}',1),clk_cnt,0))   clk_cnt_1d,
                        sum(if(a.pt = date_sub('${cur_date}',1),gmv,0))       gmv_1d,

                        --近3天
                        sum(if(a.pt >= date_sub('${cur_date}',3) and a.pt <= date_sub('${cur_date}',1),expre_cnt,0)) expre_cnt_3d,
                        sum(if(a.pt >= date_sub('${cur_date}',3) and a.pt <= date_sub('${cur_date}',1),clk_cnt,0))   clk_cnt_3d,
                        sum(if(a.pt >= date_sub('${cur_date}',3) and a.pt <= date_sub('${cur_date}',1),gmv,0))       gmv_3d,
                        --近7天
                        sum(if(a.pt >= date_sub('${cur_date}',7) and a.pt <= date_sub('${cur_date}',1),expre_cnt,0)) expre_cnt_7d,
                        sum(if(a.pt >= date_sub('${cur_date}',7) and a.pt <= date_sub('${cur_date}',1),clk_cnt,0))   clk_cnt_7d,
                        sum(if(a.pt >= date_sub('${cur_date}',7) and a.pt <= date_sub('${cur_date}',1),gmv,0))       gmv_7d,
                        --近14天
                        sum(if(a.pt >= date_sub('${cur_date}',14) and a.pt <= date_sub('${cur_date}',1),expre_cnt,0)) expre_cnt_14d,
                        sum(if(a.pt >= date_sub('${cur_date}',14) and a.pt <= date_sub('${cur_date}',1),clk_cnt,0))   clk_cnt_14d,
                        sum(if(a.pt >= date_sub('${cur_date}',14) and a.pt <= date_sub('${cur_date}',1),gmv,0))       gmv_14d,
                        --近30天
                        sum(expre_cnt) expre_cnt_30d,
                        sum(clk_cnt)   clk_cnt_30d,
                        sum(gmv)       gmv_30d
                 from dws.dws_vova_buyer_goods_behave a
                 where a.pt >= date_sub('${cur_date}', 30)
                   and a.pt <= date_sub('${cur_date}', 1)
                 group by brand_id
             ) b on a.brand_id = b.brand_id
    ) i on a.goods_id = i.goods_id

left join tmp.mlb_goods_rate_mct j on a.goods_id = j.goods_id

left join (
    select
         a.goods_id,b.avg_stay_time_mct_1d, avg_stay_time_mct_3d, avg_stay_time_mct_7d, avg_stay_time_mct_14d, avg_stay_time_mct_30d
    from dim.dim_vova_goods a
    join (
             select b.mct_id,
                    --近1天
                    sum(if(a.pt = date_sub('${cur_date}', 1) and a.leave_ts != 0 and a.enter_ts != 0,
                           if(a.leave_ts - a.enter_ts > 300 * 1000, 300 * 1000, a.leave_ts - a.enter_ts), 0)) /
                    sum(if(a.pt = date_sub('${cur_date}', 1) and a.leave_ts != 0 and a.enter_ts != 0, 1, 0)) /
                    1000 avg_stay_time_mct_1d,

                    --近3天
                    sum(if(a.pt >= date_sub('${cur_date}', 3) and a.pt <= date_sub('${cur_date}', 1) and a.leave_ts != 0 and a.enter_ts != 0,
                           if(a.leave_ts - a.enter_ts > 300 * 1000, 300 * 1000, a.leave_ts - a.enter_ts), 0)) /
                    sum(if(a.pt >= date_sub('${cur_date}', 3) and a.pt <= date_sub('${cur_date}', 1) and a.leave_ts != 0 and a.enter_ts != 0, 1, 0)) /
                    1000 avg_stay_time_mct_3d,

                    --近7天
                    sum(if(a.pt >= date_sub('${cur_date}', 7) and a.pt <= date_sub('${cur_date}', 1) and a.leave_ts != 0 and a.enter_ts != 0,
                           if(a.leave_ts - a.enter_ts > 300 * 1000, 300 * 1000, a.leave_ts - a.enter_ts), 0)) /
                    sum(if(a.pt >= date_sub('${cur_date}', 7) and a.pt <= date_sub('${cur_date}', 1) and a.leave_ts != 0 and a.enter_ts != 0, 1, 0)) /
                    1000 avg_stay_time_mct_7d,

                    --近14天
                    sum(if(a.pt >= date_sub('${cur_date}', 14) and a.pt <= date_sub('${cur_date}', 1) and a.leave_ts != 0 and a.enter_ts != 0,
                           if(a.leave_ts - a.enter_ts > 300 * 1000, 300 * 1000, a.leave_ts - a.enter_ts), 0)) /
                    sum(if(a.pt >= date_sub('${cur_date}', 14) and a.pt <= date_sub('${cur_date}', 1) and a.leave_ts != 0 and a.enter_ts != 0, 1, 0)) /
                    1000 avg_stay_time_mct_14d,

                    --近30天
                    sum(if(a.pt >= date_sub('${cur_date}', 30) and a.pt <= date_sub('${cur_date}', 1) and a.leave_ts != 0 and a.enter_ts != 0,
                           if(a.leave_ts - a.enter_ts > 300 * 1000, 300 * 1000, a.leave_ts - a.enter_ts), 0)) /
                    sum(if(a.pt >= date_sub('${cur_date}', 30) and a.pt <= date_sub('${cur_date}', 1) and a.leave_ts != 0 and a.enter_ts != 0, 1, 0)) /
                    1000 avg_stay_time_mct_30d

             from dwd.dwd_vova_log_page_view_arc a
                      join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
             where a.pt >= date_sub('${cur_date}', 30)
               and a.pt <= date_sub('${cur_date}', 1)
               and page_code = 'product_detail'
             group by b.mct_id
         ) b on a.mct_id = b.mct_id
    ) k on a.goods_id = k.goods_id
left join (select gs_id,
                  sum(gmv) gmv,
                  sum(ord_cnt) ord_cnt,
                  sum(sales_vol) sales_vol,
                  sum(add_cat_cnt) add_cat_cnt,
                  count(distinct if(add_cat_cnt > 0,buyer_id,null)) add_cat_uv
from dws.dws_vova_buyer_goods_behave
where pt = '${cur_date}'
group by gs_id
    ) l on a.goods_id = l.gs_id


"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



