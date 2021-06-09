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
msck repair table ads.ads_vova_royalty_threshold_d;
insert overwrite table dwb.dwb_vova_op_salary_thd PARTITION (pt = '${cur_month}')
select
/*+ REPARTITION(1) */
c.first_cat_name,
month_sale_threshold / dayofmonth('$cur_date') sale_threshold_d
from ads.ads_vova_royalty_threshold_d t
join dim.dim_vova_category c on t.first_cat_id = c.first_cat_id
where pt='$cur_date' and c.depth=1;

insert overwrite table dwb.dwb_vova_op_salary_goods_ok PARTITION (pt = '${cur_month}')
select
/*+ REPARTITION(1) */
t1.first_cat_name,
t1.group_id,
t1.goods_id,
t1.is_self,
t1.employee_name op,
nvl(o.mct_op_name,'') mct_op,
t3.cnt ok_days
from
(
select
t.goods_id,
t.employee_name,
if(g.group_id=-1,t.goods_id * -1,g.group_id) group_id,
g.first_cat_id,
if(g.mct_name in ('SuperAC','VogueFD'),'Y','N') is_self,
g.first_cat_name
from
(
select
employee_name,
goods_id
from
(
select
employee_name,
goods_id,
create_time,
test_result,
employee_name,
row_number() over(partition by goods_id order by create_time) rank
from ods_vova_vbd.ods_vova_test_goods_behave
) t where rank=1 and create_time>='2021-04-01 00:00:00' and test_result =1 and employee_name !='computer' and trunc(create_time,'MM')='$cur_month'
) t
left join dim.dim_vova_goods g on t.goods_id = g.goods_id
where g.brand_id =0
) t1
join
(
select
group_id,
first_cat_id
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
if(g.group_id=-1,g.goods_id * -1,g.group_id) group_id,
g.first_cat_id,
sum(p.shop_price * p.goods_number + p.shipping_fee) gmv
from dwd.dwd_vova_fact_pay p
join dim.dim_vova_goods g on p.goods_id = g.goods_id
where trunc(pay_time,'MM')='$cur_month' and g.brand_id=0
group by if(g.group_id=-1,g.goods_id * -1,g.group_id),g.first_cat_id
) t join ads.ads_vova_royalty_threshold_d r on t.first_cat_id = r.first_cat_id
where pt='$cur_date'
) t where gmv>month_sale_threshold
) t2 on t1.group_id = t2.group_id  and t1.first_cat_id = t2.first_cat_id
join
(
select
goods_id,
sum(gmv_more) cnt
from
(
select
event_date,
goods_id,
t.first_cat_name,
gmv,
if(gmv>sale_threshold_d,1,0) gmv_more
from
(
select
to_date(pay_time) event_date,
p.goods_id,
p.first_cat_name,
sum(p.shop_price * p.goods_number + p.shipping_fee) gmv
from dwd.dwd_vova_fact_pay p
join dim.dim_vova_goods g on p.goods_id = g.goods_id
where trunc(pay_time,'MM')='$cur_month' and g.brand_id=0
group by to_date(pay_time), p.goods_id,p.first_cat_name
) t join dwb.dwb_vova_op_salary_thd r on t.first_cat_name = r.first_cat_name
where pt='$cur_month'
) t group by goods_id
having cnt >7
) t3 on t1.goods_id = t3.goods_id
left join dim.dim_vova_mct_op o on t1.first_cat_name = o.first_cat_name;

insert overwrite table dwb.dwb_vova_op_salary_summary PARTITION (pt = '${cur_month}')
select
/*+ REPARTITION(1) */
first_cat_name,
count(if(is_self='Y',g.goods_id,null)) self_goods_cnt,
count(if(is_self='N',g.goods_id,null)) no_self_goods_cnt,
count(if(is_self='Y',g.goods_id,null)) * 200 + count(if(is_self='N',g.goods_id,null))* 100 op_amount,
count(if(is_self='N',g.goods_id,null))* 100 mct_op_amount
from dwb.dwb_vova_op_salary_goods_ok g
left join (select group_id from dwb.dwb_vova_op_salary_goods_ok  where pt<'${cur_month}' group by group_id) t1 on g.group_id = t1.group_id
left join (select goods_id from dwb.dwb_vova_op_salary_goods_ok  where pt<'${cur_month}' group by goods_id) t2 on g.goods_id = t2.goods_id
where pt = '${cur_month}' and t1.group_id is null and t2.goods_id is null
group by first_cat_name;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=dwb_vova_op_salary_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi