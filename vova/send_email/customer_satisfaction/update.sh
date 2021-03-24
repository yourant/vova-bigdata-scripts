#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
month_age_date=`date -d "-30 day" +%Y-%m-%d`
his_date=`date -d "-58 day" +%Y-%m-%d`
fi

spark-sql   --conf "spark.app.name=customer_satisfaction_send_email" --conf "spark.sql.autoBroadcastJoinThreshold=-1"  --conf "spark.sql.crossJoin.enabled=true"  --conf "spark.dynamicAllocation.maxExecutors=120"  -e "

insert overwrite table dwb.dwb_vova_customer_satisfaction PARTITION (pt = '${cur_date}')
select a.order_sn                                                                        order_sn,
       a.order_goods_id                                                                  order_goods_id,
       a.email                                                                           email,
       c.buyer_name                                                                      buyer_name,
       e.tel                                                                             tel,
       a.region_code                                                                     region_code,
       f.name_cn                                                                         name_cn,
       a.confirm_time                                                                    confirm_time,
       if(a.sku_shipping_status = 0, '未发货', if(a.sku_shipping_status = 1, '已发货', '已签收')) shipping_status,
       g.refund_reason                                                                   refund_type,
	   e.consignee,if(j.buyer_id is null,'N','Y') is_re_buy
from dim.dim_vova_order_goods a
         join dwd.dwd_vova_fact_pay b
              on a.order_goods_id = b.order_goods_id
         left join dim.dim_vova_buyers c
                   on a.buyer_id = c.buyer_id
         left join ods_vova_vts.ods_vova_order_info e
                   on a.order_id = e.order_id
         left join ods_vova_vts.ods_vova_languages f
                   on e.language_id = f.languages_id
         left join (select order_goods_id,refund_reason from dwd.dwd_vova_fact_refund where refund_type_id = 2) g
                   on a.order_goods_id = g.order_goods_id
         left join ods_vova_ext.ods_vova_email_unsubscribe h
                   on a.email = h.email
         join (
    select order_sn
    from (
             select a.order_sn, dense_rank() over (partition by a.region_code order by a.order_sn) rn
             from dim.dim_vova_goods a
                      join dwd.dwd_vova_fact_pay b on a.order_goods_id = b.order_goods_id
                      left join (select email from dwb.dwb_vova_customer_satisfaction where pt < '${cur_date}') c
                                on a.email = c.email
                      left join ods_vova_ext.ods_vova_email_unsubscribe h
                   on a.email = h.email
             where to_date(a.confirm_time) = '${his_date}'
               and a.datasource = 'vova'
               and a.region_code in ('GB', 'FR', 'DE', 'IT', 'ES')
               and h.email is null
               and c.email is null
         ) tmp
    where tmp.rn <= 600
    group by order_sn
) i on a.order_sn = i.order_sn
left join (select buyer_id from dwd.dwd_vova_fact_pay where to_date(pay_time) >= '$month_age_date' and to_date(pay_time) <'$cur_date' group by buyer_id) j on a.buyer_id = j.buyer_id
where to_date(a.confirm_time) = '${his_date}'
  and a.datasource = 'vova'
  and a.region_code in ('GB', 'FR', 'DE', 'IT', 'ES')
  and h.email is null
group by a.order_sn,
         a.order_goods_id,
         a.email,
         c.buyer_name,
         e.tel,
         a.region_code,
         f.name_cn,
         a.confirm_time,
         if(a.sku_shipping_status = 0, '未发货', if(a.sku_shipping_status = 1, '已发货', '已签收')),
         g.refund_reason,e.consignee,if(j.buyer_id is null,'N','Y')
;
"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
select
a.order_sn,
a.order_goods_id,
a.email,
a.buyer_name,
a.tel,
a.region_code,
a.is_re_buy,
a.name_cn,
a.confirm_time,
a.shipping_status,
a.refund_type,
a.consignee
from dwb.dwb_vova_customer_satisfaction  a
where a.pt = '${cur_date}'
"

head="
父订单号,
子订单号,
邮箱,
姓名,
电话号码,
国家,
次月是否复购,
下单语言,
订单确认时间,
发货状态,
退款原因,
收货人姓名
"

spark-submit \
--deploy-mode client \
--name 'customer_satisfaction_email_send' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "${sql}"  \
-head "${head}"  \
-receiver "juntao@vova.com.hk,suzi@vova.com.hk,sanlian@vova.com.hk,jianxiangyun@vova.com.hk" \
-title "vova 顾客满意度5个国家取数(${cur_date})" \
--type attachment \
--fileName "vova 顾客满意度5个国家取数(${cur_date})"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "发送邮件失败"
  exit 1
fi





