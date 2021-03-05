#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期当天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
select nvl(a.pt, 'all')                                                                            pt,
       nvl(a.geo_country, 'all')                                                                   geo_country,
       nvl(a.os_type, 'all')                                                                       os_type,
       nvl(case
               when (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 >= 0
                   and (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 <= 1 then '分组1'
               when (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 > 1
                   and (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 <= 24 then '分组2'
               when (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 > 24
                   and (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 <= 168 then '分组3'
               else '分组4' end
           , 'all')                                                                                fenzu,
       count(distinct a.buyer_id)                                                                  dau,
       count(distinct if(datediff(to_date(e.pay_time), a.pt) = 0, e.buyer_id, null))               pay_dau,
       count(distinct if(datediff(to_date(e.pay_time), a.pt) = 0, e.buyer_id, null)) /
       count(distinct a.buyer_id)                                                                  total_change_rate,
       count(distinct if(datediff(to_date(e.pay_time), a.pt) = 1, e.buyer_id, null))               pay_1_dau,
       count(distinct if(datediff(to_date(e.pay_time), a.pt) = 1, e.buyer_id, null)) /
       count(distinct if(datediff(d.pt, a.pt) = 1, a.buyer_id, null))                              total_change_1_rate,
       count(distinct if(datediff(to_date(e.pay_time), a.pt) <= 7, e.buyer_id, null))              pay_7_dau,
       count(distinct if(datediff(to_date(e.pay_time), a.pt) <= 7, e.buyer_id, null)) /
       count(distinct if(datediff(d.pt, a.pt) <= 7, a.buyer_id, null))                             total_change_7_rate,
       count(distinct if(datediff(d.pt, a.pt) = 1, a.buyer_id, null))                              stay_1,
       count(distinct if(datediff(d.pt, a.pt) = 2, a.buyer_id, null))                              stay_2,
       count(distinct if(datediff(d.pt, a.pt) = 3, a.buyer_id, null))                              stay_3,
       count(distinct if(datediff(d.pt, a.pt) = 4, a.buyer_id, null))                              stay_4,
       count(distinct if(datediff(d.pt, a.pt) = 5, a.buyer_id, null))                              stay_5,
       count(distinct if(datediff(d.pt, a.pt) = 6, a.buyer_id, null))                              stay_6,
       count(distinct if(datediff(d.pt, a.pt) = 7, a.buyer_id, null))                              stay_7,
       count(distinct if(datediff(d.pt, a.pt) = 1, a.buyer_id, null)) / count(distinct a.buyer_id) stay_1_rate,
       count(distinct if(datediff(d.pt, a.pt) = 2, a.buyer_id, null)) / count(distinct a.buyer_id) stay_2_rate,
       count(distinct if(datediff(d.pt, a.pt) = 3, a.buyer_id, null)) / count(distinct a.buyer_id) stay_3_rate,
       count(distinct if(datediff(d.pt, a.pt) = 4, a.buyer_id, null)) / count(distinct a.buyer_id) stay_4_rate,
       count(distinct if(datediff(d.pt, a.pt) = 5, a.buyer_id, null)) / count(distinct a.buyer_id) stay_5_rate,
       count(distinct if(datediff(d.pt, a.pt) = 6, a.buyer_id, null)) / count(distinct a.buyer_id) stay_6_rate,
       count(distinct if(datediff(d.pt, a.pt) = 7, a.buyer_id, null)) / count(distinct a.buyer_id) stay_7_rate

from dwd.dwd_vova_log_screen_view a
         left join (select buyer_id, cpn_create_time
                    from dim.dim_vova_coupon
                    where cpn_cfg_id = 1726026
                    group by buyer_id, cpn_create_time) b
                   on a.buyer_id = b.buyer_id
         join dim.dim_vova_devices c on a.device_id = c.device_id
         left join (select pt, buyer_id
                    from dwd.dwd_vova_log_screen_view a
                    where a.datasource = 'vova'
                      and a.platform = 'mob'
                      and a.os_type is not null
                      and a.device_id is not null
                      and cast(regexp_replace(a.app_version, '\\\.', 0) as bigint) >= 2010400
                      and a.geo_country in ('FR', 'DE', 'IT', 'ES')
                      and a.os_type in ('ios', 'android')
                      and a.pt >= '2021-01-27'
                    group by pt, buyer_id) d on a.buyer_id = d.buyer_id
         left join dwd.dwd_vova_fact_pay e on datediff(to_date(e.pay_time), a.pt) <= 7
    and a.buyer_id = e.buyer_id

where a.pt >= '2021-01-27'
  and a.datasource = 'vova'
  and a.platform = 'mob'
  and a.os_type is not null
  and a.os_type != ''
  and a.device_id is not null
  and cast(regexp_replace(a.app_version, '\\\.', 0) as bigint) >= 2010400
  and a.geo_country in ('FR', 'DE', 'IT', 'ES')
  and datediff(a.pt, c.activate_time) = 0
  and a.os_type in ('ios', 'android')
group by cube (a.pt, a.geo_country, a.os_type,
               case
                   when (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 >= 0
                       and (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 <= 1 then '分组1'
                   when (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 > 1
                       and (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 <= 24 then '分组2'
                   when (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 > 24
                       and (unix_timestamp(b.cpn_create_time) - unix_timestamp(c.activate_time)) / 3600 <= 168
                       then '分组3'
                   else '分组4' end)
"

head="
时间,
国家,
端,
用户分组,
dau,
支付成功uv,
整体转化率,
次日留存用户--支付成功uv,
次日留存用户-整体转化率,
7日内留存用户--支付成功uv,
7日内留存用户--整体转化率uv,
次日留存uv,
第2日留存uv,
第3日留存uv,
第4日留存uv,
第5日留存uv,
第6日留存uv,
第7日留存uv,
次留,
第2日留存率,
第3日留存率,
第4日留存率,
第5日留存率,
第6日留存率,
第7日留存率
"

spark-submit \
--deploy-mode client \
--name 'new_activate_user' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "${sql}"  \
-head "${head}"  \
-receiver "juntao@vova.com.hk" \
-title "新激活用户(${cur_date})" \
--type attachment \
--fileName "新激活用户(${cur_date})"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "发送邮件失败"
  exit 1
fi