#!/bin/bash
#指定日期和引擎
cur_date=$1
cur_month="${cur_date: 0: 7}-01"

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
cur_month=$(date -d "-1 day" +"%Y-%m-01")
fi
# 当天日期
echo "cur_date: ${cur_date}"
#当月第一天
echo "cur_month: ${cur_month}"

sql="
insert overwrite table dwb.dwb_vova_op_salary_thd PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
c.first_cat_name,
month_sale_threshold / dayofmonth('$cur_date') sale_threshold_d
from ads.ads_vova_royalty_threshold_d t
join dim.dim_vova_category c on t.first_cat_id = c.first_cat_id
where pt='$cur_date' and c.depth=1;




(
select
employee_name,
goods_id
from
(
select
employee_name,
goods_id,
row_number() over(partition by goods_id order by create_time) rank
from ods_vova_vbd.ods_vova_test_goods_behave
where create_time>='2021-04-01 00:00:00' and test_result =1 and employee_name !='computer'
) t where rank=1
) t
left join ods_vova_vbts.ods_vova_rec_gid_pic_similar gs on t.goods_id = gs.goods_id
left join
(
select
group_id,
first_cat_id,
from
(
select
t.group_id,
t.first_cat_id,
t.gmv,
r.month_sale_threshold
from
(
select
nvl(group_id,goods_id * -1),
first_cat_id,
sum(p.shop_price * p.goods_number + p.shipping_fee) gmv
from dwd.dwd_vova_fact_pay p
join ods_vova_vbts.ods_vova_rec_gid_pic_similar gs on p.goods_id = gs.goods_id
where trunc(pay_time,'MM')='$cur_month'
group by nvl(group_id,goods_id * -1),first_cat_id
) t join ads.ads_vova_royalty_threshold_d r on t.first_cat_id = r.first_cat_id
where pt='$cur_date'
) t where gmv>month_sale_threshold
) t1 on gs.group_id = t1.group_id































"