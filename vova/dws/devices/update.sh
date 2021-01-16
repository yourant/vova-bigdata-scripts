#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
-- 近60天登录的用户
drop table if exists tmp.tmp_vova_dws_device_recent_60;
create table tmp.tmp_vova_dws_device_recent_60 as
select su.datasource,
       su.device_id
from dwd.dwd_vova_fact_start_up su
where su.pt > date_sub('${cur_date}', 60)
  and su.pt <= '${cur_date}'
group by su.datasource,device_id;
drop table if exists tmp.tmp_vova_dws_device_4m_to_today;
-- 2019-04-01至今登录的用户
create table tmp.tmp_vova_dws_device_4m_to_today as
select su.datasource,
       su.device_id
from dwd.dwd_vova_fact_start_up su
where su.pt > '2019-04-01'
  and su.pt < date_sub('${cur_date}', 60)
group by su.datasource,device_id;

-- 至今-60天登录的用户
drop table if exists tmp.tmp_vova_dws_device_before_60;
create table tmp.tmp_vova_dws_device_before_60 as
select su.datasource,
       su.device_id
from dwd.dwd_vova_fact_start_up su
where su.pt = date_sub('${cur_date}', 60)
group by datasource, device_id;

-- 最后一次登录的用户，以及次日是否登录
drop table if exists tmp.tmp_vova_dws_device_last_start_up;
create table tmp.tmp_vova_dws_device_last_start_up as
select su.datasource,
       su.device_id,
       max(su.start_up_date) as last_start_up_date
from dwd.dwd_vova_fact_start_up su
where su.pt <= '${cur_date}'
group by datasource, device_id;

-- 连续登录的标签
drop table if exists tmp.tmp_vova_dws_subsequent_start_up;
create table tmp.tmp_vova_dws_subsequent_start_up as
select tmp.datasource,
       tmp.device_id,
       tmp.start_up_date,
       tmp.start_up_1d
from (select su.datasource,
             su.device_id,
             su.start_up_date,
             lag(start_up_date, 1) over (partition by datasource, device_id order by start_up_date) as start_up_1d
      from (select distinct datasource,
                            device_id,
                            start_up_date
            from dwd.dwd_vova_fact_start_up su
            where su.pt >= date_sub('${cur_date}', 1)
              and su.pt <= '${cur_date}'
           ) su
     ) as tmp
where tmp.start_up_date = '${cur_date}';


-- 计算留失用户
drop table if exists tmp.tmp_vova_dws_device_loss_device;
create table tmp.tmp_vova_dws_device_loss_device as
select dev.datasource,
       dev.device_id,
       concat(if(su.device_id is null and b60.device_id is not null, 1, 0), if(su1.device_id is null and b4.device_id is not null, 1, 0)) as loss_user
from dim.dim_vova_devices dev
         left join
     (select distinct d3.datasource,
                      d3.device_id
      from tmp.tmp_vova_dws_device_before_60 d3
               inner join tmp.tmp_vova_dws_device_recent_60 d1 on d3.device_id = d1.device_id and d3.datasource = d1.datasource
     ) as su on dev.device_id = su.device_id and su.datasource = dev.datasource
         left join
     (select distinct d2.datasource,
                      d2.device_id
      from tmp.tmp_vova_dws_device_4m_to_today d2
               inner join tmp.tmp_vova_dws_device_recent_60 d1 on d1.device_id = d2.device_id and d2.datasource = d1.datasource
     ) as su1 on dev.device_id = su1.device_id and dev.datasource = su1.datasource
         left join tmp.tmp_vova_dws_device_before_60 b60 on b60.device_id = dev.device_id and b60.datasource = dev.datasource
         left join tmp.tmp_vova_dws_device_4m_to_today b4 on b4.device_id = dev.device_id and b4.datasource = dev.datasource
where dev.device_id is not null
  and dev.datasource is not null;

-- 计算用户购买周期，价值
drop table if exists tmp.tmp_vova_dws_device_pay;
create table tmp.tmp_vova_dws_device_pay as
select oi.datasource                          as datasource,
       oi.device_id                          as device_id,
       min(oi.pay_time)                       as first_pay_time,
       max(oi.pay_time)                       as last_pay_time,
       collect_set(lp.last_1_pay_time)[0]     as last_1_pay_time,
       sum(gmv) as pay_gmv,
       count(1)                               as pay_order
from (select first(pay_time) as pay_time,
             fp.datasource,
             fp.order_id,
             fp.device_id,
             sum(goods_number * shop_price + shipping_fee) as gmv
      from dwd.dwd_vova_fact_pay fp
      where date(fp.pay_time) <= '${cur_date}'
        AND fp.datasource in ('vova', 'airyclub')
        AND fp.device_id is not null
        GROUP BY fp.device_id, fp.datasource, fp.order_id
     ) oi
         left join
     (select *
      from (select device_id,
                   datasource,
                   pay_time                                                                 as last_1_pay_time,
                   row_number() over (partition by datasource, device_id order by pay_time desc) as rank
            from (
                   select       first(pay_time) as pay_time,
                                fp.datasource,
                                fp.order_id,
                                fp.device_id
                         from dwd.dwd_vova_fact_pay fp
                         where date(fp.pay_time) <= '${cur_date}'
                           AND fp.datasource in ('vova', 'airyclub')
                           AND fp.device_id is not null
                           GROUP BY fp.device_id, fp.datasource, fp.order_id
                 ) oi
           ) as lp
      where lp.rank = 2
     ) as lp on lp.device_id = oi.device_id and lp.datasource = oi.datasource
group by oi.datasource, oi.device_id;
"
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"   --conf "spark.dynamicAllocation.maxExecutors=300"   --conf "spark.dynamicAllocation.minExecutors=60" --conf "spark.dynamicAllocation.initialExecutors=100" --conf "spark.app.name=dws_devices" --conf "spark.sql.autoBroadcastJoinThreshold=10485760" -e "$sql"

echo "prepare done, start overwrite  dws.dws_vova_devices"
sql="
-- 快照当天用户的情况
insert overwrite table dws.dws_vova_devices
select dld.datasource,
       dld.device_id,
       dp.first_pay_time,
       dp.last_pay_time,
       dp.last_1_pay_time,
       dp.pay_gmv,
       dp.pay_order,
       su.last_start_up_date,
       dld.loss_user,
       if(re.device_id is null, 0, 1)                             as is_refund,
       if(nl.device_id is null, 0, 1)                             as is_not_logistic_refund,
       case
           when dp.last_1_pay_time is null then 'R_1'
           when datediff(to_date(dp.last_pay_time), to_date(dp.last_1_pay_time)) <= 15 then 'R_1'
           when datediff(to_date(dp.last_pay_time), to_date(dp.last_1_pay_time)) <= 45 then 'R_2'
           when datediff(to_date(dp.last_pay_time), to_date(dp.last_1_pay_time)) <= 60 then 'R_3'
           else 'R_4'
           end                                                    as R_tag,
       case
           when dp.pay_order = 1 then 'F_1'
           when dp.pay_order = 2 then 'F_2'
           when dp.pay_order < 5 then 'F_3'
           else 'F_4'
           end                                                    as F_tag,
       case
           when dp.pay_order = 0 then 'M_1'
           when dp.pay_gmv / dp.pay_order <= 1 / 2 * 20 then 'M_1'
           when dp.pay_gmv / dp.pay_order <= 20 then 'M_2'
           when dp.pay_gmv / dp.pay_order <= 2 * 20 then 'M_3'
           else 'M_4'
           end                                                    as M_tag,
       if(datediff('${cur_date}', ssu.start_up_1d) = 1, 'Y', 'N') as continue_1d
from tmp.tmp_vova_dws_device_loss_device dld
         left join tmp.tmp_vova_dws_device_pay dp on dld.device_id = dp.device_id and dld.datasource = dp.datasource
         left join tmp.tmp_vova_dws_device_last_start_up su on su.device_id = dld.device_id and su.datasource = dld.datasource
         left join tmp.tmp_vova_dws_subsequent_start_up as ssu on ssu.device_id = dld.device_id and ssu.datasource = dld.datasource
         left join (select og.device_id,
                           og.datasource
                    from dwd.dwd_vova_fact_refund re
                             inner join dim.dim_vova_order_goods og on og.order_goods_id = re.order_goods_id
                    where re.exec_refund_time > '2019-04-01'
                    group by og.device_id, og.datasource) as re on re.device_id = dld.device_id and re.datasource = dld.datasource
         left join (select og.device_id,
                           og.datasource
                    from dwd.dwd_vova_fact_refund re
                             inner join dim.dim_vova_order_goods og on og.order_goods_id = re.order_goods_id
                    where re.exec_refund_time > '2019-04-01'
                      and re.refund_type_id = 2
                      and re.refund_reason_type_id != 8
                    group by og.device_id, og.datasource) as nl on nl.device_id = dld.device_id and nl.datasource = dld.datasource
where dld.datasource is not null
  and dld.datasource in ('vova', 'airyclub')
  and dld.device_id is not null;
"
hive -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

