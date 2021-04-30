#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
job_name="dwb_vova_monitor_cmb_req4544_chenkai_${cur_date}"

#计算每日海外仓商品相关信息
sql="
set hive.exec.dynamic.partition.mode=nonstrict;

with tmp_cmb_1 as (
select
  nvl(og.region_code, 'all') as region_code,
  to_date(og.order_time) as event_date,
  count(og.order_goods_id)    as tot_cnt,
  sum(if(og.mct_id in (26414,11630,36655), 1, 0))                             as my_cnt,
  sum(if(og.mct_id not in (26414,11630,36655), 1, 0))                            as oth_cnt,
  sum(if(og.mct_id in (26414,11630,36655) and vg.brand_level < 5, 1, 0))          as my_no_brd_cnt,
  sum(if(og.mct_id not in (26414,11630,36655) and vg.brand_level < 5, 1, 0))         as oth_no_brd_cnt,
  sum(if(oge.collection_plan_id = 2, 1, 0))                                as sm_cnt,
  sum(if(oge.collection_plan_id = 2 and og.mct_id in (26414,11630,36655), 1, 0))          as my_sm_cnt,
  sum(if(oge.collection_plan_id = 2 and og.mct_id not in (26414,11630,36655), 1, 0))         as oth_sm_cnt,
  sum(if(oget.extension_info > 0, 1, 0))                       as tot_mor_pay_cnt,
  sum(if(oget.extension_info > 0 and og.mct_id in (26414,11630,36655), 1, 0))  as my_mor_pay_cnt,
  sum(if(oget.extension_info > 0 and og.mct_id not in (26414,11630,36655), 1, 0)) as oth_mor_pay_cnt,
  sum(if(oge.collection_plan_id = 2 and oge.sm_id = 13, 1, 0))                    as tot_cmb_cnt,
  sum(oget.extension_info)                                     as tot_mor_pay_amt,
  sum(if(oge.collection_plan_id = 2, og.shop_price * og.goods_number + og.shipping_fee, 0))   as sm_gmv,
  count(distinct if(oge.collection_plan_id = 2, og.sku_id, null))                          as my_sku,
  nvl(to_date(og.order_time), 'all') pt
from
  dim.dim_vova_order_goods og
inner join
  dim.dim_vova_goods g using (goods_id)
inner join
  ods_vova_vts.ods_vova_order_goods_extra oge
on oge.order_goods_id = og.order_goods_id
left join
  ods_vova_vts.ods_vova_order_goods_extension oget
on oget.rec_id = og.order_goods_id and oget.ext_name = 'container_transportation_shipping_fee'
-- left join
--   dwd.dwd_vova_fact_pay fp
-- on og.order_goods_id = fp.order_goods_id
left join
  ods_vova_vts.ods_vova_goods vg
on og.goods_id = vg.goods_id
where og.sku_order_status = 1
and to_date(og.order_time) <= '${cur_date}' and to_date(og.order_time) > date_sub('${cur_date}', 7)
-- and to_date(fp.pay_time) <= '${cur_date}' and to_date(fp.pay_time) > date_sub('${cur_date}', 7)
and og.datasource = 'vova'
group by cube (og.region_code, to_date(og.order_time))
)

insert overwrite table dwb.dwb_vova_monitor_cmb PARTITION (pt)
select  /*+ REPARTITION(1) */
  a.region_code                                                              as region_code,
  event_date                                                               as event_date,
  tot_cnt                                                                  as tot_cnt,
  my_cnt                                                                   as my_cnt,
  oth_cnt                                                                  as oth_cnt,
  my_no_brd_cnt                                                            as my_no_brd_cnt,
  oth_no_brd_cnt                                                           as oth_no_brd_cnt,
  sm_cnt                                                                   as sm_cnt,
  my_sm_cnt                                                                as my_sm_cnt,
  oth_sm_cnt                                                               as oth_sm_cnt,
  tot_mor_pay_cnt                                                          as tot_mor_pay_cnt,
  my_mor_pay_cnt                                                           as my_mor_pay_cnt,
  oth_mor_pay_cnt                                                          as oth_mor_pay_cnt,
  tot_cmb_cnt                                                              as tot_cmb_cnt,
  concat(round(sm_cnt * 100 / tot_cnt, 2), '%')                            as sm_cnt_div_tot_cnt,
  concat(round(sm_cnt * 100 / (my_no_brd_cnt + oth_no_brd_cnt), 2), '%')   as sm_cnt_div_no_brd_cnt,
  concat(round((my_mor_pay_cnt + oth_mor_pay_cnt) * 100 / sm_cnt, 2), '%') as mor_pay_cnt_div_sm_cnt,
  round(tot_mor_pay_amt / tot_mor_pay_cnt, 2)                              as tot_mor_pay_amt_div_tot_mor_pay_cnt,
  tot_mor_pay_amt,
  round(sm_gmv,2) sm_gmv,
  concat(round(sm_gmv * 100 / b.gmv, 2), '%') gmv_rate,
  my_sku,
  a.pt pt
from tmp_cmb_1 a
left join
(
  select
    nvl(fp.region_code,'all') region_code,
    nvl(to_date(fp.pay_time), 'all') pt,
    sum(fp.shop_price * fp.goods_number + fp.shipping_fee) gmv
  from dwd.dwd_vova_fact_pay fp
	inner join
	  dim.dim_vova_order_goods ddog
	on ddog.order_goods_id = fp.order_goods_id
	where to_date(fp.pay_time) <= '${cur_date}' and to_date(fp.pay_time) >= date_sub('${cur_date}', 7)
	  and (fp.from_domain like '%api.vova%' or fp.from_domain like '%api.airyclub%')
		and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
	group by cube(fp.region_code, to_date(fp.pay_time))
) b
on a.region_code = b.region_code and a.pt = b.pt
where a.pt != 'all' and b.pt != 'all'
;

"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=${job_name}" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
