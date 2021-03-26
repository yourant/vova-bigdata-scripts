#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"
#默认输入每月一号的日期

sql="
with tmp_total_order as (
 select
 trunc(dog.confirm_time,'MM') as event_date,
 dog.order_goods_id,
 dog.mct_id,
 g.first_cat_id,
 if(dog.region_code in ('FR','DE','IT','ES','GB','CH','CZ','SK','AT','TW','BE','DK'), dog.region_code, '11') AS region_code,
 case when fl.collection_plan_id=2 then 3
      when fl.collection_plan_id!=2 and sc.carrier_cat=3 then 1
      when fl.collection_plan_id!=2 and sc.carrier_cat!=3 then 2
      else 4
 end as shipping_type,
 dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee    as  mct_shop_price,
 re.refund_reason_type_id,
 dog.sku_pay_status
 from dim.dim_vova_order_goods dog
 left join dim.dim_vova_category g
 on g.cat_id = dog.cat_id
 left join dwd.dwd_vova_fact_refund re
 on re.order_goods_id = dog.order_goods_id
 left join dwd.dwd_vova_fact_logistics fl on fl.order_goods_id = dog.order_goods_id
 left join dim.dim_vova_shipping_carrier sc on sc.carrier_id =  fl.shipping_carrier_id
 where dog.datasource = 'vova'
 and to_date(dog.confirm_time) <= '${cur_date}'
 and trunc(dog.confirm_time,'MM') >= trunc(add_months('${cur_date}',-2),'MM')
 and dog.mct_id > 0
 and g.first_cat_id > 0
 and dog.region_code is not null
),
tmp_total_refund_detail as (
select
    event_date,
    mct_id,
    nvl(first_cat_id,-1) as first_cat_id,
    nvl(region_code,'00') as region_code,
    nvl(shipping_type,0) as shipping_type,
    refund_amount,
    refund_number,
    item_dont_fit_cnt,
    poor_quality_cnt,
    item_not_as_described_cnt,
    defective_item_cnt,
    shipment_late_cnt,
    wrong_product_cnt,
    wrong_quantity_cnt,
    not_receive_cnt,
    others_cnt,
    empty_package_cnt
from
    (
        select
            event_date,
            mct_id,
            first_cat_id,
            region_code,
            shipping_type,
            sum(mct_shop_price) as refund_amount,
            count(order_goods_id) as refund_number,
            sum(if(refund_reason_type_id = 1,1,0)) as item_dont_fit_cnt,
            sum(if(refund_reason_type_id = 2,1,0)) as poor_quality_cnt,
            sum(if(refund_reason_type_id = 3,1,0)) as item_not_as_described_cnt,
            sum(if(refund_reason_type_id = 4,1,0)) as defective_item_cnt,
            sum(if(refund_reason_type_id = 5,1,0)) as shipment_late_cnt,
            sum(if(refund_reason_type_id = 6,1,0)) as wrong_product_cnt,
            sum(if(refund_reason_type_id = 7,1,0)) as wrong_quantity_cnt,
            sum(if(refund_reason_type_id = 8,1,0)) as not_receive_cnt,
            sum(if(refund_reason_type_id = 9,1,0)) as others_cnt,
            sum(if(refund_reason_type_id = 10,1,0)) as empty_package_cnt
        from tmp_total_order t1
        where t1.sku_pay_status = 4
          and t1.refund_reason_type_id is not null
          and shipping_type != 4
        group by
            event_date,
            mct_id,
            first_cat_id,
            region_code,
            shipping_type
            grouping sets(
            (event_date,mct_id),
            (event_date,mct_id,first_cat_id,region_code),
            (event_date,mct_id,region_code,shipping_type),
            (event_date,mct_id,first_cat_id,shipping_type),
            (event_date,mct_id,first_cat_id),
            (event_date,mct_id,region_code),
            (event_date,mct_id,shipping_type),
            (event_date,mct_id,first_cat_id,region_code,shipping_type)
            )
    ) a
),
tmp_total_order_number as (
select
    event_date,
    mct_id,
    nvl(first_cat_id,-1) as first_cat_id,
    nvl(region_code,'00') as region_code,
    nvl(shipping_type,0) as shipping_type,
    count(order_goods_id) as order_number
from tmp_total_order
        where shipping_type != 4
        group by
            event_date,
            mct_id,
            first_cat_id,
            region_code,
            shipping_type
            grouping sets(
            (event_date,mct_id),
            (event_date,mct_id,first_cat_id,region_code),
            (event_date,mct_id,region_code,shipping_type),
            (event_date,mct_id,first_cat_id,shipping_type),
            (event_date,mct_id,first_cat_id),
            (event_date,mct_id,region_code),
            (event_date,mct_id,shipping_type),
            (event_date,mct_id,first_cat_id,region_code,shipping_type)
            )
)
insert overwrite table ads.ads_vova_mct_refund_m partition (pt = '${cur_date}')
select /*+ REPARTITION(1) */
       t1.mct_id                       as mct_id,
       t1.first_cat_id                 as first_cat_id,
       t1.region_code                  as country,
       t1.shipping_type                as shipping_type,
       to_date(t1.event_date)                   as count_date,
       t1.refund_amount,
       t1.refund_number,
       nvl(t1.refund_number / t2.order_number, 0)         as refund_rate,
       nvl(t1.item_dont_fit_cnt / t2.order_number, 0)         as refund_rate_item_dont_fit,
       nvl(t1.poor_quality_cnt / t2.order_number, 0)          as refund_rate_poor_quality,
       nvl(t1.item_not_as_described_cnt / t2.order_number, 0) as refund_rate_item_not_as_described,
       nvl(t1.defective_item_cnt / t2.order_number, 0)        as refund_rate_defective_item,
       nvl(t1.shipment_late_cnt / t2.order_number, 0)         as refund_rate_shipment_late,
       nvl(t1.wrong_product_cnt / t2.order_number, 0)         as refund_rate_wrong_product,
       nvl(t1.wrong_quantity_cnt / t2.order_number, 0)        as refund_rate_wrong_quantity,
       nvl(t1.not_receive_cnt / t2.order_number, 0)           as refund_rate_not_receive,
       nvl(t1.others_cnt / t2.order_number, 0)                as refund_rate_others,
       nvl(t1.empty_package_cnt / t2.order_number, 0)         as refund_rate_empty_package
from tmp_total_refund_detail t1
        left join
    tmp_total_order_number t2
    on  t2.event_date = t1.event_date
        and t2.mct_id = t1.mct_id
        and t2.first_cat_id = t1.first_cat_id
        and t2.region_code = t1.region_code
        and t2.shipping_type = t1.shipping_type
"

spark-sql \
--conf "spark.app.name=ads_vova_mct_refund_m_yushijia" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
