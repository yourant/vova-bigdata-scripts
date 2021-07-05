##承接用户：element_name=adt_recall；未承接用户：element_name<>adt_recall

sql="
select reflect('java.net.URLDecoder', 'decode',extra) from dwd.dwd_vova_log_data where pt='2021-06-23' and event_fingerprint='113c81ae9b0c2227a24387659fdd65e2' and element_name ='adt_recall' and extra like '%adt_ids%' limit 100;


select * from (select os_type, get_json_object(reflect('java.net.URLDecoder', 'decode',extra),'$.adt_ids') sn from dwd.dwd_vova_log_data where pt='2021-06-23' and  element_name ='adt_recall' ) t where sn is not null and os_type='ios';
select * from (select os_type, get_json_object(reflect('java.net.URLDecoder', 'decode',extra),'$.adt_ids') sn from dwd.dwd_vova_log_data where pt='2021-06-23' and  element_name ='adt_recall' ) t where sn is not null and os_type='android';




with adt_recall_tmp as
(
select
pt,
device_id,
regexp_replace(sn,'[\\\\[\\\\]]','') goods_sn
from (select pt, device_id,
get_json_object(reflect('java.net.URLDecoder', 'decode',extra),'$.adt_ids') sn
from dwd.dwd_vova_log_data where pt>='2021-05-01' and  element_name ='adt_recall' and dp='vova' and datasource='vova'
) t where sn is not null
union all
select
pt,
device_id,
regexp_replace(sn,'[\\\\[\\\\]]','')  goods_sn
from (select pt, device_id,
get_json_object(reflect('java.net.URLDecoder', 'decode',extra),'$.adt_gsns') sn
from dwd.dwd_vova_log_data where pt>='2021-05-01' and  element_name ='adt_recall' and dp='vova' and datasource='vova'
) t where sn is not null
),
adt_recall_goods_id_tmp as(
select
pt,
device_id,
cast(trim(virtual_goods_id) as bigint) virtual_goods_id
from
(
select pt, device_id,goods_sn from adt_recall_tmp where lower(goods_sn) not like '%sn%'
) t LATERAL VIEW explode(split(goods_sn,',')) tmp AS virtual_goods_id
),
adt_recall_goods_sn_tmp as(
select pt, device_id,goods_sn from adt_recall_tmp where lower(goods_sn) like '%sn%'
),
adt_recall_goods_device_tmp as(
select
t.pt,
t.device_id,
t.goods_device_id,
t.goods_id,
t.virtual_goods_id
from
(
select
pt,
ad.device_id,
if(g.goods_id is not null,ad.device_id,null) goods_device_id ,
g.goods_id,
ad.virtual_goods_id
from adt_recall_goods_id_tmp ad
left join dim.dim_vova_goods g on ad.virtual_goods_id = g.virtual_goods_id
union
select
pt,
asn.device_id,
if(g.goods_id is not null,asn.device_id,null) goods_device_id,
g.goods_id,
g.virtual_goods_id
from adt_recall_goods_sn_tmp asn
left join dim.dim_vova_goods g on asn.goods_sn = g.goods_sn
) t join dim.dim_vova_devices d on t.device_id = d.device_id and d.datasource ='vova'
where datediff(t.pt,d.activate_time)<=0
),
no_adt_use as (
select
sv.pt,
sv.device_id
from dwd.dwd_vova_log_screen_view sv
join dim.dim_vova_devices d on sv.datasource = d.datasource and sv.device_id = d.device_id
left join (select pt,device_id from adt_recall_goods_device_tmp group by pt,device_id) s on sv.device_id = s.device_id and sv.pt=s.pt
where sv.pt>='2021-05-01' and dp='vova' and d.main_channel='Facebook Ads' and s.device_id is null and datediff(sv.pt,d.activate_time)<=0
group by sv.pt,sv.device_id
)
select
t1.pt,
t1.facebook_new_uv_d,
t2.no_adt_uv,
t2.no_adt_uv/t1.facebook_new_uv_d adt_goods_rate,
t3.no_adt_detail_uv,
t3.no_adt_detail_uv/t2.no_adt_uv adt_goods_detail_rate,
t4.no_adt_pay_uv,
t4.no_adt_pay_uv/t3.no_adt_detail_uv pay_zh_rate,
t4.no_adt_pay_uv/t2.no_adt_uv pay_st_rate
from
(
select
pt,
count(distinct if(datediff(sv.pt,d.activate_time)<=0,sv.device_id,null)) facebook_new_uv_d
from dwd.dwd_vova_log_screen_view sv
join dim.dim_vova_devices d on sv.datasource = d.datasource and sv.device_id = d.device_id
where pt>='2021-05-01' and dp='vova' and d.main_channel='Facebook Ads'
group by pt
) t1 join
(
select pt,count(distinct device_id) no_adt_uv from no_adt_use group by pt
) t2 on t1.pt=t2.pt
join
(
select
ad.pt,
count(distinct ad.device_id) no_adt_detail_uv
from no_adt_use ad
join (select device_id from dwd.dwd_vova_log_screen_view where pt>='2021-05-01' and dp='vova' and page_code='product_detail' group by device_id) s on ad.device_id = s.device_id
group by pt
)t3 on t1.pt=t3.pt
join
(
select
ad.pt,
count(distinct ad.device_id) no_adt_pay_uv
from no_adt_use ad
join (select to_date(pay_time) pt,device_id from dwd.dwd_vova_fact_pay where datasource='vova' and to_date(pay_time)>='2021-05-01' group by to_date(pay_time),device_id) p on ad.device_id = p.device_id and p.pt=ad.pt
group by ad.pt
) t4 on t1.pt=t4.pt






------承接用户
select
t1.pt,
t1.facebook_new_uv_d,
t2.adt_uv,
t2.adt_uv/t1.facebook_new_uv_d adt_goods_rate,
t3.adt_detail_uv,
t3.adt_detail_uv/t2.adt_uv adt_goods_detail_rate,
t4.pay_uv,
t4.pay_uv/t3.adt_detail_uv pay_zh_rate,
t4.pay_uv/t2.adt_uv pay_st_rate
from
(
select
pt,
count(distinct if(datediff(sv.pt,d.activate_time)<=0,sv.device_id,null)) facebook_new_uv_d
from dwd.dwd_vova_log_screen_view sv
join dim.dim_vova_devices d on sv.datasource = d.datasource and sv.device_id = d.device_id
where pt>='2021-05-01' and dp='vova' and d.main_channel='Facebook Ads'
group by pt
) t1 join
(
select pt,count(distinct device_id) adt_uv from adt_recall_goods_device_tmp group by pt
) t2 on t1.pt=t2.pt
join
(
select
ad.pt,
count(distinct ad.device_id) adt_detail_uv
from adt_recall_goods_device_tmp ad
join (select device_id from dwd.dwd_vova_log_screen_view where pt>='2021-05-01' and dp='vova' and page_code='product_detail' group by device_id) s on ad.device_id = s.device_id
group by pt
)t3 on t1.pt=t3.pt
join
(
select
ad.pt,
count(distinct ad.device_id) pay_uv
from adt_recall_goods_device_tmp ad
join (select to_date(pay_time) pt,device_id from dwd.dwd_vova_fact_pay where datasource='vova' and to_date(pay_time)>='2021-05-01' group by to_date(pay_time),device_id) p on ad.device_id = p.device_id and p.pt=ad.pt
group by ad.pt
) t4 on t1.pt=t4.pt



--------承接商品用户
select
t1.pt,
t1.facebook_new_uv_d,
t2.adt_goods_uv,
t2.adt_goods_uv/t1.facebook_new_uv_d adt_goods_rate,
t3.adt_goods_detail_uv,
t3.adt_goods_detail_uv/t2.adt_goods_uv adt_goods_detail_rate,
t4.pay_goods_uv,
t4.pay_goods_uv/t3.adt_goods_detail_uv pay_zh_rate,
t4.pay_goods_uv/t2.adt_goods_uv pay_st_rate
from
(
select
pt,
count(distinct if(datediff(sv.pt,d.activate_time)<=0,sv.device_id,null)) facebook_new_uv_d
from dwd.dwd_vova_log_screen_view sv
join dim.dim_vova_devices d on sv.datasource = d.datasource and sv.device_id = d.device_id
where pt>='2021-05-01' and dp='vova' and d.main_channel='Facebook Ads'
group by pt
) t1 join
(
select pt,count(distinct goods_device_id) adt_goods_uv from adt_recall_goods_device_tmp group by pt
) t2 on t1.pt=t2.pt
join
(
select
ad.pt,
count(distinct ad.goods_device_id) adt_goods_detail_uv
from adt_recall_goods_device_tmp ad
join (select device_id,virtual_goods_id from dwd.dwd_vova_log_screen_view where pt>='2021-05-01' and dp='vova' and page_code='product_detail' group by device_id,virtual_goods_id) s on ad.goods_device_id = s.device_id and ad.virtual_goods_id=s.virtual_goods_id
group by pt
)t3 on t1.pt=t3.pt
join
(
select
ad.pt,
count(distinct ad.goods_device_id) pay_goods_uv
from adt_recall_goods_device_tmp ad
join (select to_date(pay_time) pt,device_id,goods_id from dwd.dwd_vova_fact_pay where datasource='vova' and to_date(pay_time)>='2021-05-01' group by to_date(pay_time),device_id,goods_id) p on ad.goods_device_id = p.device_id and ad.goods_id = p.goods_id and p.pt=ad.pt
group by ad.pt
) t4 on t1.pt=t4.pt







"