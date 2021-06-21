#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="

select
os_type,
app_version,
geo_country,
element_type,
sum(during) / count(*) avg_during
from dwd.dwd_vova_log_performance_arc
where pt= '2021-06-14' and  element_name='startup'
group by os_type,app_version,geo_country,element_type



select
os_type,
app_version,
geo_country,
element_type,
page,
op,
op_cnt,
tot_cnt,
op_cnt/tot_cnt slow_rate
from
(
select
distinct
os_type,
app_version,
geo_country,
element_type,
page,
op,
count(op) over(partition by os_type,app_version,geo_country,element_type,page,op) op_cnt,
count(page) over(partition by os_type,app_version,geo_country,element_type,page) tot_cnt
from
(
select
os_type,
app_version,
geo_country,
element_type,
page,
if(during<=3000,'fast','slow') op,
during
from dwd.dwd_vova_log_performance_arc
where pt='2021-06-14' and element_name='operate' and during is not null
) t
) t where op ='slow'



select
os_type,
app_version,
geo_country,
element_type,
page,
op,
op_cnt,
tot_cnt,
op_cnt/tot_cnt
from
(
select
distinct
os_type,
app_version,
geo_country,
element_type,
page,
op,
count(op) over(partition by os_type,app_version,geo_country,element_type,page,op) op_cnt,
count(page) over(partition by os_type,app_version,geo_country,element_type,page) tot_cnt
from
(
select
os_type,
app_version,
geo_country,
element_type,
page,
if(during<=3000,'fast','slow') op,
during
from dwd.dwd_vova_log_performance_arc
where pt='2021-06-14' and element_name='page' and during is not null
) t
) t where op ='slow'



"