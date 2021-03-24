#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"
#默认输入每周周一的日期

sql="
insert overwrite table ads.ads_vova_mct_refund_cnt_w partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
    mct_id,
    nvl(region_code,'00') as country,
    nvl(shipping_type,0) as shipping_type,
    to_date(event_date) as count_date,
    refund_number as refund_num,
    item_dont_fit,
    poor_quality,
    item_not_as_described,
    defective_item,
    shipment_late,
    wrong_product,
    wrong_quantity,
    not_receive,
    others,
    empty_package
from
    (
        select
            date_add(dog.confirm_time,1 - case when dayofweek(dog.confirm_time) = 1 then 7 else dayofweek(dog.confirm_time) - 1 end) as event_date,
            dog.mct_id,
            if(dog.region_code in ('FR','DE','IT','ES','GB','CH','CZ','SK','AT','TW','BE','DK'), dog.region_code, '11') AS region_code,
            case when fl.collection_plan_id=2 then 3
                 when fl.collection_plan_id!=2 and sc.carrier_cat=3 then 1
      when fl.collection_plan_id!=2 and sc.carrier_cat!=3 then 2
      else 4
 end as shipping_type,
 count(dog.order_goods_id) as refund_number,
 sum(if(refund_reason_type_id = 1,1,0)) as item_dont_fit,
 sum(if(refund_reason_type_id = 2,1,0)) as poor_quality,
 sum(if(refund_reason_type_id = 3,1,0)) as item_not_as_described,
 sum(if(refund_reason_type_id = 4,1,0)) as defective_item,
 sum(if(refund_reason_type_id = 5,1,0)) as shipment_late,
 sum(if(refund_reason_type_id = 6,1,0)) as wrong_product,
 sum(if(refund_reason_type_id = 7,1,0)) as wrong_quantity,
 sum(if(refund_reason_type_id = 8,1,0)) as not_receive,
 sum(if(refund_reason_type_id = 9,1,0)) as others,
 sum(if(refund_reason_type_id = 10,1,0)) as empty_package
        from dim.dim_vova_order_goods dog
 left join dim.dim_vova_category g
 on g.cat_id = dog.cat_id
 left join dwd.dwd_vova_fact_refund re
 on re.order_goods_id = dog.order_goods_id
 left join dwd.dwd_vova_fact_logistics fl on fl.order_goods_id = dog.order_goods_id
 left join dim.dim_vova_shipping_carrier sc on sc.carrier_id =  fl.shipping_carrier_id
        where dog.datasource = 'vova'
         and to_date(dog.confirm_time) <= '${cur_date}'
         and to_date(dog.confirm_time) >= date_add(next_day('${cur_date}','Mon'),-84)
          and dog.mct_id > 0
          and dog.region_code is not null
          and dog.sku_pay_status = 4
          and re.refund_reason_type_id is not null
          and (case when fl.collection_plan_id=2 then 3
            when fl.collection_plan_id!=2 and sc.carrier_cat=3 then 1
            when fl.collection_plan_id!=2 and sc.carrier_cat!=3 then 2
            else 4
            end) != 4
        group by
            date_add(dog.confirm_time,1 - case when dayofweek(dog.confirm_time) = 1 then 7 else dayofweek(dog.confirm_time) - 1 end),
            dog.mct_id,
            if(dog.region_code in ('FR','DE','IT','ES','GB','CH','CZ','SK','AT','TW','BE','DK'), dog.region_code, '11'),
            case when fl.collection_plan_id=2 then 3
            when fl.collection_plan_id!=2 and sc.carrier_cat=3 then 1
            when fl.collection_plan_id!=2 and sc.carrier_cat!=3 then 2
            else 4
            end
        with cube
    ) a
where mct_id is not null
and event_date is not null
"

spark-sql \
--conf "spark.app.name=ads_vova_mct_refund_cnt_w_yushijia" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi