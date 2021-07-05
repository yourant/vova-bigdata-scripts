drop table if exists tmp.tmp_zyzheng_req_base_9621_0530;
create table tmp.tmp_zyzheng_req_base_9621_0530 as
select
log.pt,
log.device_id,
log.test_info,
log.extra,
get_json_object(extra, '$.element_content') AS element_content,
get_json_object(extra, '$.fastpayment') AS fastpayment,
get_json_object(extra, '$.paymentmethod') AS paymentmethod
from
dwd.dwd_vova_log_data log
WHERE log.datasource = 'vova'
AND log.platform = 'mob'
AND log.pt >= '2021-05-19'
AND log.pt <= '2021-05-30'
AND log.test_info like '%ab_9332_pay_now%'
AND log.element_name = 'order_placed'
;
select distinct element_content from tmp.tmp_zyzheng_req_base_9621_0530 log
where log.test_info like '%ab_9332_pay_now_b%'
AND log.paymentmethod IN ('credit', 'other') limit 10
;

inner join ods_vova_vts.ods_vova_order_info oi on oi.order_sn = log.element_content

select
log.pt,
count(*),
count(distinct device_id)
from
tmp.tmp_zyzheng_req_base_9621_0530 log
where log.test_info like '%ab_9332_pay_now_b%'
AND log.paymentmethod IN ('credit', 'other')
group by log.pt
;

drop table if exists tmp.tmp_zyzheng_req_base_9415_0530;
create table tmp.tmp_zyzheng_req_base_9415_0530 as
select
log.pt,
log.device_id,
log.test_info,
log.extra,
get_json_object(extra, '$.element_content') AS element_content,
get_json_object(extra, '$.fastpayment') AS fastpayment
from
dwd.dwd_vova_log_data log
WHERE log.datasource = 'vova'
AND log.platform = 'mob'
AND log.pt >= '2021-05-19'
AND log.pt <= '2021-05-30'
AND log.test_info like '%ab_9091_creditcard_fast_payment_b%'
AND log.element_name = 'order_placed'
;
select * from tmp.tmp_zyzheng_req_base_9415_0530 where fastpayment in ('on','off') limit 10;