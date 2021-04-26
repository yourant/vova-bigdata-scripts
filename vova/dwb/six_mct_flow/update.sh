#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
with tmp_vova_mct as (
select a.mct_id,c.mct_name,a.first_cat_id,b.first_cat_name,d.goods_id from  ads.ads_vova_mct_rank a
left join (select distinct first_cat_id,first_cat_name from dim.dim_vova_category) b on a.first_cat_id=b.first_cat_id
left join (select distinct mct_id,mct_name from dim.dim_vova_merchant) c on a.mct_id=c.mct_id
left join (select distinct mct_id,first_cat_id,goods_id from dim.dim_vova_goods) d on d.mct_id=a.mct_id and d.first_cat_id=a.first_cat_id
where a.pt='$cur_date' and a.rank =6
),

tmp_imp as (
select
t2.mct_name,
max(t2.first_cat_name) as first_cat_name,
t1.page,
count(*) as expre_pv,
count(distinct device_id) as expre_uv,
sum(case when rp_id='59' then 1 else 0 end) as rp_expre_pv,
COUNT(DISTINCT (case when rp_id='59' then device_id else '' end))-1 as rp_expre_uv
from (
    select
        case when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'traff_most_popular'
         when page_code ='product_detail' and list_type ='/detail_also_like' then 'traff_product_detail'
         else 'traff_others' end page,
        device_id,
        explode(split(get_rp_name(a.recall_pool),',')) rp_id,
        b.goods_id
        from
        dwd.dwd_vova_log_goods_impression a
    left join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
    where a.datasource='vova' and a.pt='$cur_date'
) t1 left join tmp_vova_mct t2 on t1.goods_id=t2.goods_id
where t2.mct_name is not null
group by t2.mct_name,
t1.page
),

tmp_clk as (
select
t2.mct_name,
t1.page,
count(*) as clk_pv,
count(distinct device_id) as clk_uv,
sum(case when rp_id='59' then 1 else 0 end) as rp_clk_pv,
COUNT(DISTINCT (case when rp_id='59' then device_id else '' end))-1 as rp_clk_uv
from (
    select
        case when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'traff_most_popular'
         when page_code ='product_detail' and list_type ='/detail_also_like' then 'traff_product_detail'
         else 'traff_others' end page,
        device_id,
        explode(split(get_rp_name(a.recall_pool),',')) rp_id,
        b.goods_id
        from
        dwd.dwd_vova_log_goods_click a
    left join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
    where a.datasource='vova' and a.pt='$cur_date'
) t1 left join tmp_vova_mct t2 on t1.goods_id=t2.goods_id
where t2.mct_name is not null
group by t2.mct_name,
t1.page
),

tmp_pay(
select
t2.mct_name,
t1.page,
count(*) as order_cnt,
sum(total_amount) as gmv,
count(distinct buyer_id) as pay_uv,
sum(case when rp_id='59' then 1 else 0 end) as rp_order_cnt,
sum(case when rp_id='59' then total_amount else 0.00 end) as rp_gmv,
count(distinct (case when rp_id='59' then buyer_id else '' end))-1 as rp_pay_uv
from
(select
fp.goods_id,
case when ocv.pre_page_code in ('homepage','product_list') and ocv.pre_list_type in ('/product_list_popular','/product_list') then 'traff_most_popular'
 when ocv.pre_page_code ='product_detail' and ocv.pre_list_type ='/detail_also_like' then 'traff_product_detail'
 else 'traff_others' end page,
(fp.shop_price*fp.goods_number+fp.shipping_fee) as total_amount,
fp.buyer_id,
explode(split(get_rp_name(ocv.pre_recall_pool),',')) rp_id
from
dwd.dwd_vova_fact_order_cause_v2 ocv
inner join dwd.dwd_vova_fact_pay fp on ocv.order_goods_id = fp.order_goods_id
where ocv.pt='$cur_date'
and ocv.datasource='vova') t1 left join tmp_vova_mct t2 on t1.goods_id=t2.goods_id
where t2.mct_name is not null
group by t2.mct_name,
t1.page
)

insert overwrite table dwb.dwb_vova_six_mct_flow_monitor partition(pt='$cur_date')
select
nvl(a.mct_name,'all') as mct_name,
nvl(a.first_cat_name,'all') as first_cat_name,
nvl(a.page,'all') as page,
sum(a.expre_pv) as expre_pv,
sum(a.expre_uv) as expre_uv,
sum(a.rp_expre_pv) as rp_expre_pv,
sum(a.rp_expre_uv) as rp_expre_uv,
concat(round(sum(a.rp_expre_pv)*100/sum(a.expre_pv),2),'%') as rp_expre_rate,
sum(b.clk_pv) as clk_pv,
sum(b.clk_uv) as clk_uv,
sum(b.rp_clk_pv) as rp_clk_pv,
sum(b.rp_clk_uv) as rp_clk_uv,
concat(round(sum(b.clk_pv)*100/sum(a.expre_pv),2),'%') as ctr,
concat(round(sum(b.rp_clk_pv)*100/sum(a.expre_uv),2),'%') as rp_ctr,
sum(c.order_cnt) as order_cnt,
sum(c.pay_uv) as pay_uv,
sum(c.gmv) as gmv,
sum(c.rp_order_cnt) as rp_order_cnt,
sum(c.rp_pay_uv) as rp_pay_uv,
sum(c.rp_gmv) as rp_gmv,
concat(round(sum(c.rp_gmv)*100/sum(c.gmv),2),'%') as rp_gmv_rate
from tmp_imp a
left join tmp_clk b on a.mct_name=b.mct_name and a.page=b.page
left join tmp_pay c on a.mct_name=c.mct_name and a.page=c.page
group by
a.mct_name,
a.first_cat_name,
a.page
with cube;

with tmp_vova_mct as (
select a.mct_id,c.mct_name,a.first_cat_id,b.first_cat_name,d.goods_id from  ads.ads_vova_mct_rank a
left join (select distinct first_cat_id,first_cat_name from dim.dim_vova_category) b on a.first_cat_id=b.first_cat_id
left join (select distinct mct_id,mct_name from dim.dim_vova_merchant) c on a.mct_id=c.mct_id
left join (select distinct mct_id,first_cat_id,goods_id from dim.dim_vova_goods) d on d.mct_id=a.mct_id and d.first_cat_id=a.first_cat_id
where a.pt='$cur_date' and a.rank =6
),

tmp_goods_cnt as (
select
goods_id,
page,
rp_name,
sum(expre_cnt) as expre_cnt,
sum(clk_cnt) as clk_cnt,
sum(order_cnt) as order_cnt,
round(sum(gmv),2) as gmv
from (
select
a.goods_id,
case when a.page_code in ('homepage','product_list') and a.list_type in ('/product_list_popular','/product_list') then 'traff_most_popular'
when a.page_code ='product_detail' and a.list_type ='/detail_also_like' then 'traff_product_detail'
else 'traff_others' end page,
a.rp_name,
a.expre_cnt,
a.clk_cnt,
a.order_cnt,
a.gmv
from
ads.ads_vova_goods_imp_detail a
where a.pt = '$cur_date' ) T
group by goods_id,page,rp_name
)

insert overwrite table dwb.dwb_vova_six_mct_goods_rp_flow_monitor partition(pt='$cur_date')
select
/*+ REPARTITION(1) */
nvl(a.mct_name,'all') as mct_name,    --店铺名称
nvl(b.page,'all') as page,         --页面
round(avg(b.expre_cnt)) as avg_expre_pv, --单个商品平均流量
max(b.expre_cnt) as max_expre_pv, --单个商品最大流量
min(b.expre_cnt) as min_expre_pv, --单个商品最小流量
round(sum(b.gmv)/count(distinct a.goods_id),4) as avg_gmv, --单个商品平均gmv
round(max(b.gmv),4) as max_gmv, --单个商品最大gmv
sum(case when b.expre_cnt>0 and b.order_cnt<1 then 1 else 0 end) as expre_no_order_cnt,--有曝光单未出单商品个数
sum(case when b.expre_cnt>0 and b.order_cnt>0 then 1 else 0 end) as expre_order_cnt --有曝光且出单商品个数
from tmp_vova_mct a
left join tmp_goods_cnt b on a.goods_id=b.goods_id
where b.rp_name='59'
group by a.mct_name,b.page with cube;


with tmp_vova_mct as (
select a.mct_id,c.mct_name,a.first_cat_id,b.first_cat_name,d.goods_id from  ads.ads_vova_mct_rank a
left join (select distinct first_cat_id,first_cat_name from dim.dim_vova_category) b on a.first_cat_id=b.first_cat_id
left join (select distinct mct_id,mct_name from dim.dim_vova_merchant) c on a.mct_id=c.mct_id
left join (select distinct mct_id,first_cat_id,goods_id from dim.dim_vova_goods) d on d.mct_id=a.mct_id and d.first_cat_id=a.first_cat_id
where a.pt='$cur_date' and a.rank =6
)

insert overwrite table dwb.dwb_vova_six_mct_block_reason partition(pt='$cur_date')
select
/*+ REPARTITION(1) */
mct_name,
goods_id,
block_reason from (
select
a.mct_name,
a.goods_id,
case when c.block_reason = 'a' then '商品流量达到上限'
    when c.block_reason = 'b' then '商家流量达到上限'
    when c.block_reason = 'c' then '页面流量达到上限'
    when c.block_reason = 'd' then 'ctr不达标'
    when c.block_reason = 'e' then '未出单'
    when c.block_reason = 'f' then '1w gcr不达标'
    when c.block_reason = 'g' then '2w gcr不达标'
else '' end  as block_reason
from tmp_vova_mct a
left join ads.ads_vova_six_mct_flow_support_goods_behave_h c on a.goods_id=c.goods_id
where c.pt = '$cur_date' and c.block_reason != 'normal'
) a where a.block_reason != '';
"
spark-sql --conf "spark.app.name=dwb_vova_six_mct_flow" --conf "spark.dynamicAllocation.maxExecutors=50" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
