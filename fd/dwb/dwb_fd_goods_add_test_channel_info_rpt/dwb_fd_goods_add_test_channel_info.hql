CREATE TABLE IF NOT EXISTS `dwb.dwb_fd_goods_add_test_channel_info`(
  `project_name` string COMMENT '组织',
  `platform` string COMMENT '平台',
  `country` string COMMENT '国家',
  `cat_id` bigint COMMENT '品类id',
  `cat_name` string COMMENT '品类',
  `ga_channel` string COMMENT '投放渠道',
  `add_session_id` string COMMENT '加车 session id',
  `view_session_id` string COMMENT 'view session id',
  `order_id` bigint COMMENT '订单id',
  `goods_amount` DECIMAL(15, 4) COMMENT '订单金额',
  `goods_test_goods_id` bigint COMMENT '测款商品id',
  `success_goods_test_goods_id` bigint COMMENT '测款成功商品id',
  `success_order_id` bigint COMMENT '测款成功的订单id',
  `success_goods_amount` DECIMAL(15, 4) COMMENT '测款成功的订单金额')
COMMENT '商品加购测款渠道信息表'
PARTITIONED BY (
  `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


INSERT overwrite table dwb.dwb_fd_goods_add_test_channel_info PARTITION (pt = '${hiveconf:pt}')
select t.project_name,
       t.platform,
       t.country,
       t.cat_id,
       t.cat_name,
       s.ga_channel,
       t.add_session_id,
       t.view_session_id,
       null as order_id,
       null as goods_amount,
       null as goods_test_goods_id,
       null as success_goods_test_goods_id,
       null as success_order_id,
       null as success_goods_amount
from (
select 
        project_name,
        platform,
        country,
        cat_id,
        cat_name,
        if(add_session_id is null,view_session_id, add_session_id) as session_id,
        view_session_id,
        add_session_id,
        pt
    from dwd.dwd_fd_goods_add_info
    where pt <= '${hiveconf:pt}'
    and cat_name is not null
) t
    left join
(
    select session_id, ga_channel
    from dwd.dwd_fd_session_channel
    where pt = '${hiveconf:pt}'
) s on t.session_id = s.session_id

union all
select project_name,
       case
           when platform = 'mob' then 'APP'
           when platform = 'web' and platform_type = 'pc_web' then 'PC'
           when platform = 'others' and platform_type = 'others' then 'others'
           else 'H5' end as platform,
       country_code,
       cat_id,
       cat_name,
       ga_channel,
       null                      as add_session_id,
       null                      as view_session_id,
       order_id,
       goods_number * shop_price as goods_amount,
       null                      as goods_test_goods_id,
       null                      as success_goods_test_goods_id,
       null                      as success_order_id,
       null                      as success_goods_amount
from dwd.dwd_fd_order_channel_analytics
where  date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${hiveconf:pt}'
  and pay_status = 2

union all
select a.project_name,
       a.platform_name    as platform,
       a.country_code,
       a.cat_id,
       a.cat_name,
       b.ga_channel,
       null               as add_session_id,
       null               as view_session_id,
       null               as order_id,
       null               as goods_amount,
       a.virtual_goods_id as goods_test_goods_id,
       if(a.result = 1,a.virtual_goods_id,null)  as success_goods_test_goods_id,
       null               as success_order_id,
       null               as success_goods_amount
from (select project_name,platform_name,country_code,cat_id,cat_name,virtual_goods_id,result from dwd.dwd_fd_finished_goods_test where to_date(finish_time) = '${hiveconf:pt}' ) a
left join (select virtual_goods_id,project_name,ga_channel from dwd.dwd_fd_order_channel_analytics where to_date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${hiveconf:pt}') b on a.project_name = b.project_name and a.virtual_goods_id = b.virtual_goods_id

union all
select a.project_name,
       a.platform_type as platform,
       a.country_code,
       a.cat_id,
       a.cat_name,
       b.ga_channel,
       null            as add_session_id,
       null            as view_session_id,
       null            as order_id,
       null            as goods_amount,
       null            as goods_test_goods_id,
       null            as success_goods_test_goods_id,
       b.success_order_id,
       b.success_goods_amount
from (
         select project_name,
                platform_name as platform_type,
                country_code,
                cat_id,
                cat_name,
                virtual_goods_id,
                create_time
         from dwd.dwd_fd_finished_goods_test
         where to_date(finish_time) <= '${hiveconf:pt}'
           and result = 1
     ) a
         inner join
     (
         select project_name,
                case
                    when platform = 'mob' then 'APP'
                    when platform = 'web' and platform_type = 'pc_web' then 'PC'
                    when platform = 'others' and platform_type = 'others' then 'others '
                    else 'H5' end                                as platform,
                country_code,
                cat_id,
                cat_name,
                ga_channel,
                order_id                                         as success_order_id,
                cast(virtual_goods_id as int)                    as virtual_goods_id,
                from_unixtime(order_time, 'yyyy-MM-dd HH:mm:ss') as order_time,
                (goods_number * shop_price)                      as success_goods_amount
         from dwd.dwd_fd_order_channel_analytics
           where  date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${hiveconf:pt}'
           and pay_status = 2
     ) b on b.project_name = a.project_name
         and b.platform = a.platform_type and b.country_code = a.country_code and b.cat_id = a.cat_id and a.virtual_goods_id = cast(b.virtual_goods_id as int)
     where a.create_time <= b.order_time;