#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
yester_date=`date -d "${cur_date} -1days" +"%Y-%m-%d"`
last7_date=`date -d "${cur_date} -14days" +"%Y-%m-%d"`

echo $cur_date
echo $yester_date
echo $last7_date


sql="
alter table dwb.dwb_vova_loss_user_collect drop if exists partition (pt = '${yester_date}');
insert overwrite table dwb.dwb_vova_loss_user_collect partition (pt = '${cur_date}')
select
date_sub('${cur_date}',10) cur_date,
nvl(tmp.region_code,'all') region_code,
       nvl(tmp.platform,'all') platform,
       nvl(tmp.is_new_user,'all') is_new_user,
       nvl(tmp.is_activate_user,'all') is_activate_user,
       count(distinct pv) dau,
       count(distinct 10_unload) date_10,
       0 date_11,
       0 date_12,
       0 date_13,
       0 date_14,
       0 date_15,
       0 date_16,
       0 date_17
from (select
       nvl(b.region_code,'NA') region_code,
       nvl(b.platform,'NA') platform,
       if(c.device_id is null,'Y','N') is_new_user,
       if(to_date(b.activate_time) = date_sub('${cur_date}',10),'Y','N') is_activate_user,
       a.device_id pv,
       if(d.device_id is null,a.device_id,null) 10_unload
from dwd.dwd_vova_fact_start_up a
left join dim.dim_vova_devices b
on a.device_id = b.device_id
left join (select device_id from  dwd.dwd_vova_fact_pay group by device_id) c
on a.device_id = c.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt > date_sub('${cur_date}',10) and pt <= '${cur_date}' group by device_id ) d
on a.device_id = d.device_id
where a.pt = date_sub('${cur_date}',10)) tmp group by cube (tmp.region_code,tmp.platform,tmp.is_activate_user,tmp.is_new_user)
union all
select
date_sub('${cur_date}',11) cur_date,
nvl(tmp.region_code,'all') region_code,
       nvl(tmp.platform,'all') platform,
       nvl(tmp.is_new_user,'all') is_new_user,
       nvl(tmp.is_activate_user,'all') is_activate_user,
       count(distinct pv) dau,
       count(distinct 10_unload) date_10,
       count(distinct 11_unload) date_11,
       0 date_12,
       0 date_13,
       0 date_14,
       0 date_15,
       0 date_16,
       0 date_17
from (select
       nvl(b.region_code,'NA') region_code,
       nvl(b.platform,'NA') platform,
       if(c.device_id is null,'Y','N') is_new_user,
       if(to_date(b.activate_time) = date_sub('${cur_date}',11),'Y','N') is_activate_user,
       a.device_id pv,
       if(d.device_id is null,a.device_id,null) 10_unload,
       if(d.device_id is null and e.device_id is null,a.device_id,null) 11_unload
from dwd.dwd_vova_fact_start_up a
left join dim.dim_vova_devices b
on a.device_id = b.device_id
left join (select device_id from  dwd.dwd_vova_fact_pay group by device_id) c
on a.device_id = c.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt > date_sub('${cur_date}',11) and pt <= date_sub('${cur_date}',1) group by device_id ) d
on a.device_id = d.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = '${cur_date}' group by device_id ) e
on a.device_id = e.device_id
where a.pt = date_sub('${cur_date}',11)) tmp group by cube (tmp.region_code,tmp.platform,tmp.is_activate_user,tmp.is_new_user)
union all
select
date_sub('${cur_date}',12) cur_date,
nvl(tmp.region_code,'all') region_code,
       nvl(tmp.platform,'all') platform,
       nvl(tmp.is_new_user,'all') is_new_user,
       nvl(tmp.is_activate_user,'all') is_activate_user,
       count(distinct pv) dau,
       count(distinct 10_unload) date_10,
       count(distinct 11_unload) date_11,
       count(distinct 12_unload) date_12,
       0 date_13,
       0 date_14,
       0 date_15,
       0 date_16,
       0 date_17
from (select
       nvl(b.region_code,'NA') region_code,
       nvl(b.platform,'NA') platform,
       if(c.device_id is null,'Y','N') is_new_user,
       if(to_date(b.activate_time) = date_sub('${cur_date}',12),'Y','N') is_activate_user,
       a.device_id pv,
       if(d.device_id is null,a.device_id,null) 10_unload,
       if(d.device_id is null and e.device_id is null,a.device_id,null) 11_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null,a.device_id,null) 12_unload
from dwd.dwd_vova_fact_start_up a
left join dim.dim_vova_devices b
on a.device_id = b.device_id
left join (select device_id from  dwd.dwd_vova_fact_pay group by device_id) c
on a.device_id = c.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt > date_sub('${cur_date}',12) and pt <= date_sub('${cur_date}',2) group by device_id ) d
on a.device_id = d.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',1) group by device_id ) e
on a.device_id = e.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = '${cur_date}' group by device_id) f
on a.device_id = f.device_id
where a.pt = date_sub('${cur_date}',12)) tmp group by cube (tmp.region_code,tmp.platform,tmp.is_activate_user,tmp.is_new_user)
union all
select
date_sub('${cur_date}',13) cur_date,
nvl(tmp.region_code,'all') region_code,
       nvl(tmp.platform,'all') platform,
       nvl(tmp.is_new_user,'all') is_new_user,
       nvl(tmp.is_activate_user,'all') is_activate_user,
       count(distinct pv) dau,
       count(distinct 10_unload) date_10,
       count(distinct 11_unload) date_11,
       count(distinct 12_unload) date_12,
       count(distinct 13_unload) date_13,
       0 date_14,
       0 date_15,
       0 date_16,
       0 date_17
from (select
       nvl(b.region_code,'NA') region_code,
       nvl(b.platform,'NA') platform,
       if(c.device_id is null,'Y','N') is_new_user,
       if(to_date(b.activate_time) = date_sub('${cur_date}',13),'Y','N') is_activate_user,
       a.device_id pv,
       if(d.device_id is null,a.device_id,null) 10_unload,
       if(d.device_id is null and e.device_id is null,a.device_id,null) 11_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null,a.device_id,null) 12_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null,a.device_id,null) 13_unload
from dwd.dwd_vova_fact_start_up a
left join dim.dim_vova_devices b
on a.device_id = b.device_id
left join (select device_id from  dwd.dwd_vova_fact_pay group by device_id) c
on a.device_id = c.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt > date_sub('${cur_date}',13) and pt <= date_sub('${cur_date}',3) group by device_id ) d
on a.device_id = d.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',2) group by device_id ) e
on a.device_id = e.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',1) group by device_id) f
on a.device_id = f.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = '${cur_date}' group by device_id ) g
on a.device_id = g.device_id
where a.pt = date_sub('${cur_date}',13)) tmp group by cube (tmp.region_code,tmp.platform,tmp.is_activate_user,tmp.is_new_user)
union all
select
date_sub('${cur_date}',14) cur_date,
nvl(tmp.region_code,'all') region_code,
       nvl(tmp.platform,'all') platform,
       nvl(tmp.is_new_user,'all') is_new_user,
       nvl(tmp.is_activate_user,'all') is_activate_user,
       count(distinct pv) dau,
       count(distinct 10_unload) date_10,
       count(distinct 11_unload) date_11,
       count(distinct 12_unload) date_12,
       count(distinct 13_unload) date_13,
       count(distinct 14_unload) date_14,
       0 date_15,
       0 date_16,
       0 date_17
from (select
       nvl(b.region_code,'NA') region_code,
       nvl(b.platform,'NA') platform,
       if(c.device_id is null,'Y','N') is_new_user,
       if(to_date(b.activate_time) = date_sub('${cur_date}',14),'Y','N') is_activate_user,
       a.device_id pv,
       if(d.device_id is null,a.device_id,null) 10_unload,
       if(d.device_id is null and e.device_id is null,a.device_id,null) 11_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null,a.device_id,null) 12_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null,a.device_id,null) 13_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null and h.device_id is null,a.device_id,null) 14_unload
from dwd.dwd_vova_fact_start_up a
left join dim.dim_vova_devices b
on a.device_id = b.device_id
left join (select device_id from  dwd.dwd_vova_fact_pay group by device_id) c
on a.device_id = c.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt > date_sub('${cur_date}',14) and pt <= date_sub('${cur_date}',4) group by device_id ) d
on a.device_id = d.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',3) group by device_id ) e
on a.device_id = e.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',2) group by device_id) f
on a.device_id = f.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',1) group by device_id ) g
on a.device_id = g.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = '${cur_date}' group by device_id ) h
on a.device_id = h.device_id
where a.pt = date_sub('${cur_date}',14)) tmp group by cube (tmp.region_code,tmp.platform,tmp.is_activate_user,tmp.is_new_user)
union all
select
date_sub('${cur_date}',15) cur_date,
nvl(tmp.region_code,'all') region_code,
       nvl(tmp.platform,'all') platform,
       nvl(tmp.is_new_user,'all') is_new_user,
       nvl(tmp.is_activate_user,'all') is_activate_user,
       count(distinct pv) dau,
       count(distinct 10_unload) date_10,
       count(distinct 11_unload) date_11,
       count(distinct 12_unload) date_12,
       count(distinct 13_unload) date_13,
       count(distinct 14_unload) date_14,
       count(distinct 15_unload) date_15,
       0 date_16,
       0 date_17
from (select
       nvl(b.region_code,'NA') region_code,
       nvl(b.platform,'NA') platform,
       if(c.device_id is null,'Y','N') is_new_user,
       if(to_date(b.activate_time) = date_sub('${cur_date}',15),'Y','N') is_activate_user,
       a.device_id pv,
       if(d.device_id is null,a.device_id,null) 10_unload,
       if(d.device_id is null and e.device_id is null,a.device_id,null) 11_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null,a.device_id,null) 12_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null,a.device_id,null) 13_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null and h.device_id is null,a.device_id,null) 14_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null and h.device_id is null and i.device_id is null,a.device_id,null) 15_unload
from dwd.dwd_vova_fact_start_up a
left join dim.dim_vova_devices b
on a.device_id = b.device_id
left join (select device_id from  dwd.dwd_vova_fact_pay group by device_id) c
on a.device_id = c.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt > date_sub('${cur_date}',15) and pt <= date_sub('${cur_date}',5) group by device_id ) d
on a.device_id = d.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',4) group by device_id ) e
on a.device_id = e.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',3) group by device_id) f
on a.device_id = f.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',2) group by device_id ) g
on a.device_id = g.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',1) group by device_id ) h
on a.device_id = h.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = '${cur_date}' group by device_id ) i
on a.device_id = i.device_id
where a.pt = date_sub('${cur_date}',15)) tmp group by cube (tmp.region_code,tmp.platform,tmp.is_activate_user,tmp.is_new_user)
union all
select
date_sub('${cur_date}',16) cur_date,
nvl(tmp.region_code,'all') region_code,
       nvl(tmp.platform,'all') platform,
       nvl(tmp.is_new_user,'all') is_new_user,
       nvl(tmp.is_activate_user,'all') is_activate_user,
       count(distinct pv) dau,
       count(distinct 10_unload) date_10,
       count(distinct 11_unload) date_11,
       count(distinct 12_unload) date_12,
       count(distinct 13_unload) date_13,
       count(distinct 14_unload) date_14,
       count(distinct 15_unload) date_15,
       count(distinct 16_unload) date_16,
       0 date_17
from (select
       nvl(b.region_code,'NA') region_code,
       nvl(b.platform,'NA') platform,
       if(c.device_id is null,'Y','N') is_new_user,
       if(to_date(b.activate_time) = date_sub('${cur_date}',16),'Y','N') is_activate_user,
       a.device_id pv,
       if(d.device_id is null,a.device_id,null) 10_unload,
       if(d.device_id is null and e.device_id is null,a.device_id,null) 11_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null,a.device_id,null) 12_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null,a.device_id,null) 13_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null and h.device_id is null,a.device_id,null) 14_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null and h.device_id is null and i.device_id is null,a.device_id,null) 15_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null and h.device_id is null and i.device_id is null and j.device_id is null,a.device_id,null) 16_unload
from dwd.dwd_vova_fact_start_up a
left join dim.dim_vova_devices b
on a.device_id = b.device_id
left join (select device_id from  dwd.dwd_vova_fact_pay group by device_id) c
on a.device_id = c.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt > date_sub('${cur_date}',16) and pt <= date_sub('${cur_date}',6) group by device_id ) d
on a.device_id = d.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',5) group by device_id ) e
on a.device_id = e.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',4) group by device_id) f
on a.device_id = f.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',3) group by device_id ) g
on a.device_id = g.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',2) group by device_id ) h
on a.device_id = h.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',1) group by device_id ) i
on a.device_id = i.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = '${cur_date}' group by device_id ) j
on a.device_id = j.device_id
where a.pt = date_sub('${cur_date}',16)) tmp group by cube (tmp.region_code,tmp.platform,tmp.is_activate_user,tmp.is_new_user)
union all
select
date_sub('${cur_date}',17) cur_date,
nvl(tmp.region_code,'all') region_code,
       nvl(tmp.platform,'all') platform,
       nvl(tmp.is_new_user,'all') is_new_user,
       nvl(tmp.is_activate_user,'all') is_activate_user,
       count(distinct pv) dau,
       count(distinct 10_unload) date_10,
       count(distinct 11_unload) date_11,
       count(distinct 12_unload) date_12,
       count(distinct 13_unload) date_13,
       count(distinct 14_unload) date_14,
       count(distinct 15_unload) date_15,
       count(distinct 16_unload) date_16,
       count(distinct 17_unload) date_17
from (select
       nvl(b.region_code,'NA') region_code,
       nvl(b.platform,'NA') platform,
       if(c.device_id is null,'Y','N') is_new_user,
       if(to_date(b.activate_time) = date_sub('${cur_date}',17),'Y','N') is_activate_user,
       a.device_id pv,
       if(d.device_id is null,a.device_id,null) 10_unload,
       if(d.device_id is null and e.device_id is null,a.device_id,null) 11_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null,a.device_id,null) 12_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null,a.device_id,null) 13_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null and h.device_id is null,a.device_id,null) 14_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null and h.device_id is null and i.device_id is null,a.device_id,null) 15_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null and h.device_id is null and i.device_id is null and j.device_id is null,a.device_id,null) 16_unload,
       if(d.device_id is null and e.device_id is null and f.device_id is null and g.device_id is null and h.device_id is null and i.device_id is null and j.device_id is null and k.device_id is null,a.device_id,null) 17_unload
from dwd.dwd_vova_fact_start_up a
left join dim.dim_vova_devices b
on a.device_id = b.device_id
left join (select device_id from  dwd.dwd_vova_fact_pay group by device_id) c
on a.device_id = c.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt > date_sub('${cur_date}',17) and pt <= date_sub('${cur_date}',7) group by device_id ) d
on a.device_id = d.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',6) group by device_id ) e
on a.device_id = e.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',5) group by device_id) f
on a.device_id = f.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',4) group by device_id ) g
on a.device_id = g.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',3) group by device_id ) h
on a.device_id = h.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',2) group by device_id ) i
on a.device_id = i.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = date_sub('${cur_date}',1) group by device_id ) j
on a.device_id = j.device_id
left join (select device_id from dwd.dwd_vova_fact_start_up where pt = '${cur_date}' group by device_id  ) k
on a.device_id = k.device_id
where a.pt = date_sub('${cur_date}',17)) tmp group by cube (tmp.region_code,tmp.platform,tmp.is_activate_user,tmp.is_new_user)
;

--从当日起往前推40天到最近10天前登陆的用户，之后连续10天没登陆app的用户


insert overwrite table  dwb.dwb_vova_loss_user_loss PARTITION (pt = '${cur_date}')
select
    '${cur_date}' cur_date,
     nvl(region_code,'all') region_code,
     nvl(platform,'all') platform,
    nvl(is_new_user,'all') is_new_user,
       nvl(is_activate_user,'all') is_activate_user,
       count(distinct 30m_uv) uv_30m,
       count(distinct 60m_uv) uv_60m,
       count(distinct 2h_uv) uv_2h,
       count(distinct 3h_uv) uv_3h,
       count(distinct 1d_uv) uv_1d,
       count(distinct 3d_uv) uv_3d,
       count(distinct 7d_uv) uv_7d,
       count(distinct 15d_uv) uv_15d,
       count(distinct 30d_uv) uv_30d,
       count(distinct c_30m_uv) c_30m_uv,
       count(distinct c_60m_uv) c_60m_uv,
       count(distinct c_3h_uv) c_3h_uv,
       count(distinct c_6h_uv) c_6h_uv,
       count(distinct c_24h_uv) c_24h_uv,
       count(distinct c_3d_uv) c_3d_uv,
       count(distinct c_7d_uv) c_7d_uv,
       count(distinct c_15d_uv) c_15d_uv,
       count(distinct c_30d_uv) c_30d_uv
from (select
       nvl(a.region_code,'NA') region_code,
       nvl(a.platform,'NA') platform,
       if(c.device_id is null,'Y','N') is_new_user,
       if(to_date(a.activate_time) = '${cur_date}','Y','N') is_activate_user,
       case when (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 < 30 then a.device_id else null end 30m_uv,
       case when (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 >= 30 and
                 (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 < 60 then a.device_id else null end 60m_uv,
       case when (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 >= 60 and
                 (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 < 120 then a.device_id else null end 2h_uv,
       case when (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 >= 120 and
                 (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 < 180 then a.device_id else null end 3h_uv,
       case when (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 >= 3 and
                 (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 < 24 then a.device_id else null end 1d_uv,
       case when (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 >= 1 and
                 (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 3 then a.device_id else null end 3d_uv,
       case when (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 >= 3 and
                 (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 7 then a.device_id else null end 7d_uv,
       case when (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 >= 7 and
                 (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 15 then a.device_id else null end 15d_uv,
       case when (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 >= 15 and
                 (unix_timestamp(b.last_load_date,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 30 then a.device_id else null end 30d_uv,
            case when (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) > 0 and
                      (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 < 30 then a.device_id else null end c_30m_uv,
            case when (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 >= 30 and
                 (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 < 60 then a.device_id else null end c_60m_uv,
            case when (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 >= 60 and
                 (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 < 180 then a.device_id else null end c_3h_uv,
            case when (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 >= 3 and
                 (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 < 6 then a.device_id else null end c_6h_uv,
            case when (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 >= 6 and
                 (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 < 24 then a.device_id else null end c_24h_uv,
            case when (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 >= 1 and
                 (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 3 then a.device_id else null end c_3d_uv,
            case when (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 >= 3 and
                 (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 7 then a.device_id else null end c_7d_uv,
            case when (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 >= 7 and
                 (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 15 then a.device_id else null end c_15d_uv,
            case when (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 >= 15 and
                 (unix_timestamp(a.first_order_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.activate_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 30 then a.device_id else null end c_30d_uv
from dim.dim_vova_devices a
join (
select device_id,
       last_load_date
from (select device_id,
             max(max_collector_time) last_load_date
      from dwd.dwd_vova_fact_start_up
      where pt >= date_sub('${cur_date}', 40)
      group by device_id) tmp
where last_load_date < date_sub('${cur_date}', 10)
) b
on a.device_id = b.device_id
left join (select device_id from  dwd.dwd_vova_fact_pay group by device_id) c
on a.device_id = c.device_id) tmp group by cube (tmp.region_code,tmp.platform,tmp.is_activate_user,tmp.is_new_user);
"


#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.app.name=dwb_vova_lose_user" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"




