#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
drop table if exists tmp.tmp_vova_buyer_max_visits_cw_his;
create table tmp.tmp_vova_buyer_max_visits_cw_his as
select
datasource,
buyer_id,
cnt
from
(
select
datasource,
buyer_id,
cnt,
row_number() over(partition by datasource,buyer_id order by cnt desc ) rank
from
(
select
datasource,
year(pt) yr,
weekofyear(pt) wk,
buyer_id,
count(distinct pt) cnt
from dwd.dwd_vova_fact_log_screen_view
group by datasource,year(pt),weekofyear(pt),buyer_id
) t1
) where rank = 1;
"