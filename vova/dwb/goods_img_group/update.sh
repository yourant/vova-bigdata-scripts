#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
insert overwrite table dwb.dwb_vova_goods_img_group_d PARTITION (pt = '${pt}')
select
/*+ REPARTITION(1) */
t.event_date,
t.one_gp_cnt,
t1.gp_cnt,
t2.g_cnt
from
(
select
'$pt' event_date,
count(*) one_gp_cnt
from
(
select
group_id,
count(*) cnt
from dim.dim_vova_goods
where group_id>0
group by group_id
having cnt=1
) t
) t join
(
select
'$pt' event_date,
count(distinct group_id) gp_cnt
from dim.dim_vova_goods where group_id>0
) t1 on t.event_date =t1.event_date
join
(
select
'$pt' event_date,
count(*) g_cnt
from dim.dim_vova_goods
where is_on_sale=1
) t2 on t.event_date =t2.event_date

"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=dwb_vova_goods_img_group_d_zhangyin" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
