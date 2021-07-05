
#start
drop table if exists tmp.tmp_zyzheng_req_9459_impre;
create table tmp.tmp_zyzheng_req_9459_impre as
select
t.pt,
t.device_id,
t.rec_page_code,
ab_test
from
(
select
log.device_id,
log.pt,
case
when log.page_code = 'homepage' and log.list_type = '/popular' then 'rec_best_selling'
when log.page_code in ('homepage', 'product_list') and log.list_type in ('/product_list_popular', '/product_list') then 'rec_most_popular'
else 'others' end AS rec_page_code,
split(test_info,'&') test_info
from
dwd.dwd_vova_log_goods_impression log
where log.pt >= '2021-05-01'
  and log.pt <= '2021-05-08'
  and log.datasource = 'vova'
  and log.platform = 'mob'
  and (
  (
  log.page_code in ('homepage') AND log.list_type in ('/popular')
  )
  or
  (
  log.page_code in ('homepage', 'product_list') AND log.list_type in ('/product_list_popular', '/product_list')
  )
  )
) t LATERAL VIEW explode(t.test_info)  ab_tes as ab_test
where ab_test in (
'rec_home_srg_gqa',
'rec_home_srg_gqb',
'rec_home_srg_gqc',
'rec_mp_srg_dd',
'rec_mp_srg_h'
)
;

drop table if exists tmp.tmp_zyzheng_req_9459_clk;
create table tmp.tmp_zyzheng_req_9459_clk as
select
/*+ REPARTITION(50) */
t.pt,
t.device_id,
t.rec_page_code,
ab_test
from
(
select
log.device_id,
log.pt,
case
when log.page_code = 'homepage' and log.list_type = '/popular' then 'rec_best_selling'
when log.page_code in ('homepage', 'product_list') and log.list_type in ('/product_list_popular', '/product_list') then 'rec_most_popular'
else 'others' end AS rec_page_code,
split(test_info,'&') test_info
from
dwd.dwd_vova_log_goods_click log
where log.pt >= '2021-05-01'
  and log.pt <= '2021-05-08'
  and log.datasource = 'vova'
  and log.platform = 'mob'
  and (
  (
  log.page_code in ('homepage') AND log.list_type in ('/popular')
  )
  or
  (
  log.page_code in ('homepage', 'product_list') AND log.list_type in ('/product_list_popular', '/product_list')
  )
  )
) t LATERAL VIEW explode(t.test_info)  ab_tes as ab_test
where ab_test in (
'rec_home_srg_gqa',
'rec_home_srg_gqb',
'rec_home_srg_gqc',
'rec_mp_srg_dd',
'rec_mp_srg_h'
)
;

drop table if exists tmp.tmp_zyzheng_req_9459_cart;
create table tmp.tmp_zyzheng_req_9459_cart as
select
t.pt,
t.device_id,
t.rec_page_code,
ab_test
from
(
select
log.device_id,
log.pt,
case
when log.pre_page_code = 'homepage' and log.pre_list_type = '/popular' then 'rec_best_selling'
when log.pre_page_code in ('homepage', 'product_list') and log.pre_list_type in ('/product_list_popular', '/product_list') then 'rec_most_popular'
else 'others' end AS rec_page_code,
split(pre_test_info,'&') test_info
from
dwd.dwd_vova_fact_cart_cause_v2 log
where log.pt >= '2021-05-01'
  and log.pt <= '2021-05-08'
  and log.datasource = 'vova'
  and (
  (
  log.pre_page_code in ('homepage') AND log.pre_list_type in ('/popular')
  )
  or
  (
  log.pre_page_code in ('homepage', 'product_list') AND log.pre_list_type in ('/product_list_popular', '/product_list')
  )
  )
) t LATERAL VIEW explode(t.test_info)  ab_tes as ab_test
where ab_test in (
'rec_home_srg_gqa',
'rec_home_srg_gqb',
'rec_home_srg_gqc',
'rec_mp_srg_dd',
'rec_mp_srg_h'
)
;

drop table if exists tmp.tmp_zyzheng_req_9459_order;
create table tmp.tmp_zyzheng_req_9459_order as
select
t.pt,
t.order_goods_id,
t.device_id,
t.rec_page_code,
ab_test
from
(
select
log.pt,
log.order_goods_id,
log.device_id,
case
when log.pre_page_code = 'homepage' and log.pre_list_type = '/popular' then 'rec_best_selling'
when log.pre_page_code in ('homepage', 'product_list') and log.pre_list_type in ('/product_list_popular', '/product_list') then 'rec_most_popular'
else 'others' end AS rec_page_code,
split(pre_test_info,'&') test_info
from
dwd.dwd_vova_fact_order_cause_v2 log
where log.pt >= '2021-05-01'
  and log.pt <= '2021-05-08'
  and log.datasource = 'vova'
  and (
  (
  log.pre_page_code in ('homepage') AND log.pre_list_type in ('/popular')
  )
  or
  (
  log.pre_page_code in ('homepage', 'product_list') AND log.pre_list_type in ('/product_list_popular', '/product_list')
  )
  )
) t LATERAL VIEW explode(t.test_info)  ab_tes as ab_test
where ab_test in (
'rec_home_srg_gqa',
'rec_home_srg_gqb',
'rec_home_srg_gqc',
'rec_mp_srg_dd',
'rec_mp_srg_h'
)
;


########################
#final sql
select
t1.pt,
t1.rec_page_code,
t1.is_new,
t1.ab_test,
t1.impre_pv,
t1.impre_uv,
t2.impre_pv AS clk_pv,
t2.impre_uv AS clk_uv,
round(t2.impre_pv / t1.impre_pv,4) AS ctr,
t3.impre_uv AS cart_uv,
round(t3.impre_uv / t1.impre_uv,4) AS add_cart_rate,
t4.order_cnt,
-- t4.paid_order_cnt,
t4.paid_buyer_cnt,
round(t4.paid_buyer_cnt / t1.impre_uv,4) AS pay_uv_expre_uv,
t4.gmv,
round(t4.gmv / t1.impre_uv, 4) AS gmv_cr
from
(
select
nvl(t1.pt, 'all') AS pt,
nvl(t1.rec_page_code, 'all') AS rec_page_code,
nvl(t1.ab_test, 'all') AS ab_test,
nvl(if(datediff(t1.pt, dd.activate_time) <= 10, 'Y', 'N'), 'all') AS is_new,
count(*) AS impre_pv,
count(distinct t1.device_id) AS impre_uv
from
tmp.tmp_zyzheng_req_9459_impre t1
left join dim.dim_vova_devices dd on dd.device_id = t1.device_id AND dd.datasource = 'vova'
group by cube (if(datediff(t1.pt, dd.activate_time) <= 10, 'Y', 'N'), t1.pt, t1.rec_page_code, t1.ab_test)
having pt != 'all' and rec_page_code != 'all' and ab_test != 'all'
) t1
left join
(
select
nvl(t1.pt, 'all') AS pt,
nvl(t1.rec_page_code, 'all') AS rec_page_code,
nvl(t1.ab_test, 'all') AS ab_test,
nvl(if(datediff(t1.pt, dd.activate_time) <= 10, 'Y', 'N'), 'all') AS is_new,
count(*) AS impre_pv,
count(distinct t1.device_id) AS impre_uv
from
tmp.tmp_zyzheng_req_9459_clk t1
left join dim.dim_vova_devices dd on dd.device_id = t1.device_id AND dd.datasource = 'vova'
group by cube (if(datediff(t1.pt, dd.activate_time) <= 10, 'Y', 'N'), t1.pt, t1.rec_page_code, t1.ab_test)
having pt != 'all' and rec_page_code != 'all' and ab_test != 'all'
) t2 on t1.pt = t2.pt AND t1.rec_page_code = t2.rec_page_code AND t1.ab_test = t2.ab_test
and t1.is_new = t2.is_new
left join
(
select
nvl(t1.pt, 'all') AS pt,
nvl(t1.rec_page_code, 'all') AS rec_page_code,
nvl(t1.ab_test, 'all') AS ab_test,
nvl(if(datediff(t1.pt, dd.activate_time) <= 10, 'Y', 'N'), 'all') AS is_new,
count(*) AS impre_pv,
count(distinct t1.device_id) AS impre_uv
from
tmp.tmp_zyzheng_req_9459_cart t1
left join dim.dim_vova_devices dd on dd.device_id = t1.device_id AND dd.datasource = 'vova'
group by cube (if(datediff(t1.pt, dd.activate_time) <= 10, 'Y', 'N'), t1.pt, t1.rec_page_code, t1.ab_test)
having pt != 'all' and rec_page_code != 'all' and ab_test != 'all'
) t3 on t1.pt = t3.pt AND t1.rec_page_code = t3.rec_page_code AND t1.ab_test = t3.ab_test
and t1.is_new = t3.is_new
left join
(
select
nvl(t1.pt, 'all') AS pt,
nvl(t1.rec_page_code, 'all') AS rec_page_code,
nvl(t1.ab_test, 'all') AS ab_test,
nvl(if(datediff(t1.pt, dd.activate_time) <= 10, 'Y', 'N'), 'all') AS is_new,
count(*) AS order_cnt,
count(distinct if(dog.pay_status >=1 ,t1.order_goods_id, null)) AS paid_order_cnt,
count(distinct if(dog.pay_status >=1 ,t1.device_id, null)) AS paid_buyer_cnt,
sum(distinct if(dog.pay_status >=1 ,dog.shipping_fee + dog.shop_price * dog.goods_number , 0)) AS gmv
from
tmp.tmp_zyzheng_req_9459_order t1
INNER JOIN dim.dim_vova_order_goods dog on dog.order_goods_id = t1.order_goods_id
left join dim.dim_vova_devices dd on dd.device_id = t1.device_id AND dd.datasource = 'vova'
where dog.parent_order_id = 0
group by cube (if(datediff(t1.pt, dd.activate_time) <= 10, 'Y', 'N'), t1.pt, t1.rec_page_code, t1.ab_test)
having pt != 'all' and rec_page_code != 'all' and ab_test != 'all'
) t4 on t1.pt = t4.pt AND t1.rec_page_code = t4.rec_page_code AND t1.ab_test = t4.ab_test
and t1.is_new = t4.is_new
;
#end




select datediff('2021-05-01', '2021-05-04')








select
distinct rec_page_code
from dwd.dwd_vova_ab_test_expre log
where log.pt = '2021-05-01'
  and log.datasource = 'vova' limit 10;
  and log.platform = 'mob'
  and log.rec_page_code in ('rec_best_selling', 'rec_most_popular') limit 10
  and log.rec_code = 'rec_home_srg' limit 10;

  and concat(log.rec_code, '_', log.rec_version) in (
'rec_home_srg_gqa',
'rec_home_srg_gqb',
'rec_home_srg_gqc',
'rec_mp_srg_dd',
'rec_mp_srg_h'
  )
limit 10























