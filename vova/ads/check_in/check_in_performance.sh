#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

sql="
--dau
insert overwrite table tmp.tmp_check_in_dau
select activate_range,
       region_code,
       gmv_stage,
       count(distinct device_id) as dau
from (
         select a.device_id,
                case
                    when a.device_id is null then 5
                    when to_date(dd.activate_time) = '${cur_date}' then 1
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
                    else 4
                    end                   as activate_range,
                nvl(dd.region_code, 'NA') as region_code,
                nvl(y.gmv_stage,5) as gmv_stage,
                row_number()                 over(partition by a.device_id order by a.buyer_id desc) rk
         from dwd.dwd_vova_fact_start_up a
                  left join dim.dim_vova_devices dd
                            on a.device_id = dd.device_id
                                and a.datasource = dd.datasource
                  left join (select *
                             from ads.ads_vova_buyer_portrait_feature
                             where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                            on y.buyer_id = a.buyer_id
                                and a.datasource = y.datasource
         where a.datasource = 'vova'
           and to_date(a.pt) = '${cur_date}'
     ) t1
where rk = 1
group by activate_range,
         region_code,
         gmv_stage with cube
;

-- 签到页UV + 下单率
insert overwrite table tmp.tmp_check_in_d_uv
select activate_range,
       region_code,
       gmv_stage,
       count(distinct d)                                    as check_in_uv,
       nvl(count(distinct pd) / count(distinct d), 0) * 100 as order_rate
from (
         select a.device_id               as d,
                pay.device_id             as pd,
                case
                    when a.device_id is null then 5
                    when to_date(dd.activate_time) = '${cur_date}' then 1
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
                    else 4
                    end                   as activate_range,
                nvl(dd.region_code, 'NA') as region_code,
                nvl(y.gmv_stage,5) as gmv_stage,
                row_number()                 over(partition by a.device_id order by a.buyer_id desc) rk
         from dwd.dwd_vova_log_screen_view a
                  left join dim.dim_vova_devices dd
                            on a.device_id = dd.device_id
                                and a.datasource = dd.datasource
                  left join (select *
                    from ads.ads_vova_buyer_portrait_feature
                    where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                            on y.buyer_id = a.buyer_id
                                and a.datasource = y.datasource
                  left join dwd.dwd_vova_fact_pay pay
                            on a.buyer_id = pay.buyer_id
                                and a.pt = to_date(pay.pay_time)
                                and a.datasource = pay.datasource
                                and pay.datasource = a.datasource
         where a.datasource = 'vova'
           and a.pt = '${cur_date}'
           and a.page_code = 'coins_rewards'
     ) t2
where rk = 1
group by activate_range,
         region_code,
         gmv_stage with cube
;

--发放积分
insert overwrite table tmp.tmp_check_in_distribution
select activate_range,
       region_code,
       gmv_stage,
       sum(mission_give_check_in) as mission_give_check_in,
       sum(mission_give_all)      as mission_give_all
from (
         select t3.*,
                case
                    when t3.device_id is null then 5
                    when to_date(dd.activate_time) = '${cur_date}' then 1
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
                    else 4
                    end                   as activate_range,
                nvl(dd.region_code, 'NA') as region_code,
                nvl(y.gmv_stage,5) as gmv_stage
         from (
                  select a.user_id as buyer_id,
                         a.mission_give_check_in,
                         a.mission_give_all,
                         b.device_id
                  from (select umr.user_id,
                               sum(
                                       if(umr.mission_id = 7, cast(umr.mission_reward_value as bigint), 0)) as mission_give_check_in,
                               sum(cast(umr.mission_reward_value as bigint))                                as mission_give_all
                        FROM ods_vova_vts.ods_vova_user_mission_record umr
                        WHERE umr.is_complete = 1
                          and umr.mission_reward_type = 'coins'
                          AND to_date(umr.create_time) = '${cur_date}'
                          AND umr.mission_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9)
                          AND umr.from_domain LIKE '%api%'
                        group by umr.user_id
                       ) a
                           left join (
                      select device_id,
                             user_id
                      from (
                               select device_id,
                                      user_id,
                                      row_number() over(partition by umr.user_id order by umr.device_id desc) rk
                               FROM ods_vova_vts.ods_vova_user_mission_record umr
                               WHERE umr.is_complete = 1
                                 and umr.mission_reward_type = 'coins'
                                 AND to_date(umr.create_time) = '${cur_date}'
                                 AND umr.mission_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9)
                                 AND umr.from_domain LIKE '%api%'
                                 and umr.device_id is not null
                           ) tmp
                      where rk = 1
                  ) b
                                     on a.user_id = b.user_id
              ) t3
                  left join dim.dim_vova_devices dd
                            on t3.device_id = dd.device_id
                            and dd.datasource = 'vova'
                  left join (select *
                    from ads.ads_vova_buyer_portrait_feature
                    where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                            on y.buyer_id = t3.buyer_id
                            and y.datasource = 'vova'
     ) tmp
group by activate_range,
         region_code,
         gmv_stage with cube
;

--消耗积分
insert overwrite table tmp.tmp_check_in_cost
select activate_range,
       region_code,
       gmv_stage,
       sum(lottery_cost) as lottery_cost,
       sum(coupon_cost)  as coupon_cost
from (
         select t4.*,
                case
                    when t4.device_id is null then 5
                    when to_date(dd.activate_time) = '${cur_date}' then 1
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
                    else 4
                    end                   as activate_range,
                nvl(dd.region_code, 'NA') as region_code,
                nvl(y.gmv_stage,5) as gmv_stage
         from (
                  select a.*,
                         b.device_id
                  from (
                           select user_id,
                                  sum(if(exchange_type = 'lottery', old_value - value, 0)) AS lottery_cost,
                                  sum(if(exchange_type = 'coupon', old_value - value, 0))  AS coupon_cost
                           from ods_vova_vts.ods_vova_user_wallet_ops_log uwol
                           WHERE ops_field = 'coins'
                             AND ops = 'exchange'
                             AND to_date(uwol.create_time) = '${cur_date}'
                             AND uwol.from_domain LIKE '%api%'
                           group by uwol.user_id
                       ) a
                           left join
                       (
                           select user_id, device_id
                           from (
                                    select uwol.user_id,
                                           uwol.device_id,
                                           row_number() over(partition by uwol.user_id order by uwol.device_id desc) rk
                                    from ods_vova_vts.ods_vova_user_wallet_ops_log uwol
                                    WHERE ops_field = 'coins'
                                      AND ops = 'exchange'
                                      AND to_date(uwol.create_time) = '${cur_date}'
                                      AND uwol.from_domain LIKE '%api%'
                                ) tmp
                           where rk = 1) b
                       on a.user_id = b.user_id
              ) t4
                  left join dim.dim_vova_devices dd
                            on t4.device_id = dd.device_id
                            and dd.datasource = 'vova'
                            left join
                  (select *
                    from ads.ads_vova_buyer_portrait_feature
                    where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                            on y.buyer_id = t4.user_id
                            and y.datasource = 'vova'
     ) tmp
group by activate_range,
         region_code,
         gmv_stage with cube
;

insert overwrite table ads.ads_vova_check_in_d PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */
    '${cur_date}'              as event_date,
    case
        when activate_range is null then 'all'
        when activate_range = 1 then '新激活'
        when activate_range = 2 then '2-7天'
        when activate_range = 3 then '8-30天'
        when activate_range = 4 then '30天+'
        else '无激活信息'
        end                    as activate_range,
    if(region_code is null,'all',region_code) as region_code,
    if(gmv_stage is null,'all',cast(gmv_stage as string)) as gmv_stage,
    sum(dau)                   as dau,
    sum(check_in_uv)           as check_in_uv,
    sum(order_rate)            as order_rate,
    sum(mission_give_check_in) as mission_give_check_in,
    sum(mission_give_all)      as mission_give_all,
    sum(lottery_cost)          as lottery_cost,
    sum(coupon_cost)           as coupon_cost
from (
         select activate_range,
                region_code,
                gmv_stage,
                dau as dau,
                0   as check_in_uv,
                0   as order_rate,
                0   as mission_give_check_in,
                0   as mission_give_all,
                0   as lottery_cost,
                0   as coupon_cost
         from tmp.tmp_check_in_dau
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0           as dau,
                check_in_uv as check_in_uv,
                order_rate  as order_rate,
                0           as mission_give_check_in,
                0           as mission_give_all,
                0           as lottery_cost,
                0           as coupon_cost
         from tmp.tmp_check_in_d_uv
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0                     as dau,
                0                     as check_in_uv,
                0                     as order_rate,
                mission_give_check_in as mission_give_check_in,
                mission_give_all      as mission_give_all,
                0                     as lottery_cost,
                0                     as coupon_cost
         from tmp.tmp_check_in_distribution
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0            as dau,
                0            as check_in_uv,
                0            as order_rate,
                0            as mission_give_check_in,
                0            as mission_give_all,
                lottery_cost as lottery_cost,
                coupon_cost  as coupon_cost
         from tmp.tmp_check_in_cost
     ) tmp
group by activate_range,
         region_code,
         gmv_stage
;
"
spark-sql \
--conf "spark.app.name=ads_vova_check_in_d_huachen" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
