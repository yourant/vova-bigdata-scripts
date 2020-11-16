CREATE TABLE IF NOT EXISTS tmp.tmp_fd_snowplow_order (
`project_name` string COMMENT '组织',
`country` string COMMENT '国家',
`platform_type` string COMMENT '平台',
`page_code` string COMMENT 'page_code',
`list_type` string COMMENT 'list_type',
`user_id` string COMMENT '订单 用户id',
`domain_userid` string COMMENT '打点 设备id',
`order_id` string COMMENT '订单id',
`pay_status` string COMMENT '支付状态',
`order_amount` decimal(15,4) COMMENT '订单金额包含运费'
) COMMENT '打点数据和订单数据相关'
PARTITIONED BY (`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

CREATE TABLE IF NOT EXISTS tmp.tmp_fd_snowplow_order_base (
`project_name` string COMMENT '组织',
`country` string COMMENT '国家',
`platform_type` string COMMENT '平台',
`page_code` string COMMENT 'page_code',
`list_type` string COMMENT 'list_type',
`user_id` string COMMENT '订单 用户id',
`domain_userid` string COMMENT '打点 设备id',
`order_id` string COMMENT '订单id',
`pay_status` string COMMENT '支付状态',
`order_amount` decimal(10,2) COMMENT '订单金额包含运费'
) COMMENT '打点数据和订单数据相关'
PARTITIONED BY (`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

/*打点和订单 临时表*/
insert overwrite table tmp.tmp_fd_snowplow_order partition (dt = '${hiveconf:dt}')
select
    /*+ MAPJOIN(t1) */
    if(t2.domain_userid is not null,t2.project_name,t3.project_name) as project_name,
    if(t2.domain_userid is not null,t2.country,t3.country) as country,
    if(t2.domain_userid is not null,t2.platform_type,t3.platform_type) as platform_type,
    if(t2.domain_userid is not null,t2.page_code,t3.page_code) as page_code,
    if(t2.domain_userid is not null,t2.list_type,t3.list_type) as list_type,
    t1.user_id,
    t1.domain_userid as domain_userid,
    t1.order_id,
    t1.pay_status,
    cast(t1.gmv as decimal(10,2)) as order_amount
from(

    /* 下单数 */
    SELECT
     ogi.user_id,
     ud.sp_duid as domain_userid,
     ogi.order_id,
     9 as pay_status,
     null as gmv
    FROM (

        select user_id,order_id
         from ods_fd_vb.ods_fd_order_info
         where  date(from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss')) = '${hiveconf:dt}'
         and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
         group by user_id,order_id

    ) ogi
    LEFT JOIN (
        SELECT oud.user_id,oud.sp_duid
        FROM (
         SELECT
             user_id,
             sp_duid,
             created_time,
             from_unixtime(created_time) as create_time_utc,
             last_update_time as update_time ,
             from_unixtime(last_update_time) as last_update_time, /*洛杉矶*/
             row_number() over (partition by user_id order by last_update_time desc) as rn
         FROM ods_fd_vb.ods_fd_user_duid
         WHERE  sp_duid is not null
        ) oud where oud.rn = 1

    ) ud ON CAST(ogi.user_id AS string) = CAST(ud.user_id AS string) where ud.sp_duid is not null

    /* 支付订单 */
    union all
    SELECT
         tab1.user_id,
         tab1.sp_duid as domain_userid,
         tab1.order_id,
         tab1.pay_status,
         sum(tab1.goods_amount + tab1.shipping_fee) as gmv
    FROM (
        SELECT
         ogi.user_id,
         ud.sp_duid,
         ogi.order_id,
         ogi.pay_status,
         ogi.goods_amount,
         ogi.shipping_fee
        FROM (
            select user_id,order_id,goods_amount,shipping_fee,pay_status
            from ods_fd_vb.ods_fd_order_info
            where (date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${hiveconf:dt}'
		or date(from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss')) = '${hiveconf:dt}')
            and pay_status = 2
            and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"

        ) ogi
        LEFT JOIN (
            SELECT oud.user_id,oud.sp_duid
            FROM (
             SELECT
                 user_id,
                 sp_duid,
                 created_time,
                 from_unixtime(created_time) as create_time_utc,
                 last_update_time as update_time ,
                 from_unixtime(last_update_time) as last_update_time, /*洛杉矶*/
                 row_number() over (partition by user_id order by last_update_time desc) as rn
             FROM  ods_fd_vb.ods_fd_user_duid
             WHERE sp_duid is not null
            ) oud where oud.rn = 1

        ) ud ON cast(ogi.user_id as string) = cast(ud.user_id as string) where ud.sp_duid is not null

    ) tab1 group by tab1.user_id,tab1.sp_duid,tab1.order_id,tab1.pay_status


)t1
left join (
    SELECT t0.project_name,t0.country,t0.platform_type,t0.page_code,t0.list_type,t0.domain_userid
    from (
        SELECT
            project_name,
            upper(country) as country,
            platform_type,
            page_code,
            list_type,
            domain_userid,
            row_number() over (partition by domain_userid order by derived_tstamp desc) as rn
        from tmp.tmp_fd_snowplow_click_impr
        where dt >= date_sub('${hiveconf:dt}',60) and dt <= '${hiveconf:dt}'
        AND event_name = 'goods_click'
        AND page_code != 'afterPay'
    ) t0 where t0.rn = 1
)t2 on t1.domain_userid = t2.domain_userid
left join (
    SELECT t0.project_name,t0.country,t0.platform_type,t0.page_code,t0.list_type,t0.domain_userid
    from (
        SELECT
            project_name,
            upper(country) as country,
            platform_type,
            page_code,
            list_type,
            domain_userid,
            row_number() over (partition by domain_userid order by derived_tstamp desc) as rn
        from tmp.tmp_fd_snowplow_click_impr
        where dt = '${hiveconf:dt}'
        AND event_name = 'goods_impression'
        AND page_code != 'afterPay'
    ) t0 where t0.rn = 1
)t3 on t1.domain_userid = t3.domain_userid;

/*打点和订单 结果表*/
insert overwrite table tmp.tmp_fd_snowplow_order_base partition (dt = '${hiveconf:dt}')
select
    project_name,
    upper(country) as country,
    platform_type,
    page_code,
    list_type,
    user_id,
    domain_userid,
    order_id,
    pay_status,
    order_amount
from tmp.tmp_fd_snowplow_order
where dt = '${hiveconf:dt}' and project_name is not null
distribute by pmod(cast(rand()*1000 as int),1);

/* 删除临时表前一天的数据*/
alter table tmp.tmp_fd_snowplow_order drop partition (dt = '${hiveconf:dt}');
