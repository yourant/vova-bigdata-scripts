sql="
select count(*),count(distinct device_id) from dwd.dwd_vova_log_data where pt='2021-06-27' and element_name='RegisterVisitorSuccessApi'



select
t1.buyer_id,
dvce_created_tstamp-data_dvce_created_tstamp ts
from
(select buyer_id,first(dvce_created_tstamp) data_dvce_created_tstamp, first(collector_tstamp) data_collector_tstamp from dwd.dwd_vova_log_data where pt='2021-06-27' and element_name='RegisterVisitorSuccessApi' group by buyer_id) t1
left join
(select buyer_id,first(dvce_created_tstamp) dvce_created_tstamp, first(collector_tstamp) collector_tstamp from dwd.dwd_vova_log_screen_view where pt='2021-06-27' and page_code='product_detail' group by buyer_id) t2 on t1.buyer_id = t2.buyer_id




select
count(*)
from
(select buyer_id,first(dvce_created_tstamp) data_dvce_created_tstamp, first(collector_tstamp) data_collector_tstamp from dwd.dwd_vova_log_data where pt='2021-06-27' and element_name='RegisterVisitorSuccessApi' group by buyer_id) t1
left join
(select buyer_id,first(dvce_created_tstamp) dvce_created_tstamp, first(collector_tstamp) collector_tstamp from dwd.dwd_vova_log_screen_view where pt='2021-06-27' and page_code='product_detail' group by buyer_id) t2 on t1.buyer_id = t2.buyer_id




"