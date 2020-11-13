CREATE TABLE IF NOT EXISTS dwb.dwb_fd_repurchase_report (
    project_name STRING COMMENT '网站组织'
    ,country_code STRING COMMENT '国家缩写'
    ,platform STRING COMMENT '用户访问平台类型（web or mob）'
    ,device_type STRING COMMENT '用户设备类型'
    ,window_sig STRING COMMENT '用户最早下单时间'
    ,`window` STRING
    ,base BIGINT
    ,w1 BIGINT
    ,w2 BIGINT
    ,w3 BIGINT
    ,w4 BIGINT
    ,w5 BIGINT
    ,w6 BIGINT
) COMMENT '计算每月购买相关数据表'
PARTITIONED BY(
    dt STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc


INSERT OVERWRITE TABLE dwb.dwb_fd_repurchase_report PARTITION (dt='${hiveconf:dt}')
SELECT
    u1.project_name
    ,u1.country_code
    ,u1.platform
    ,u1.device_type
    ,from_unixtime(unix_timestamp(u1.window_sig, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM') AS window_sig
    ,concat('w', (u2.order_window - CAST(from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'yyyy') * 12 + from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'MM') AS INT))) AS `window`
    ,if(u2.order_window = u1.order_window + 0, 1, 0) AS base
    ,if(u2.order_window = CAST(from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'yyyy') * 12 + from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'MM') AS INT) + 1, 1, 0) AS w1
    ,if(u2.order_window = CAST(from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'yyyy') * 12 + from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'MM') AS INT) + 2, 1, 0) AS w2
    ,if(u2.order_window = CAST(from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'yyyy') * 12 + from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'MM') AS INT) + 3, 1, 0) AS w3
    ,if(u2.order_window = CAST(from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'yyyy') * 12 + from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'MM') AS INT) + 4, 1, 0) AS w4
    ,if(u2.order_window = CAST(from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'yyyy') * 12 + from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'MM') AS INT) + 5, 1, 0) AS w5
    ,if(u2.order_window = CAST(from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'yyyy') * 12 + from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'MM') AS INT) + 6, 1, 0) AS w6
FROM
    dwd.dwd_fd_user_window_buy u1
    JOIN dwd.dwd_fd_user_window_buy u2 ON (u1.dt = u2.dt and u1.email = u2.email and u2.order_window >= u1.order_window)
WHERE
    u1.window_sig >= from_unixtime(unix_timestamp(add_months('${hiveconf:dt}', -6), 'yyyy-MM-dd'), 'yyyy-MM-01');


CREATE VIEW IF NOT EXISTS dwb.dwb_fd_repurchase_report_view AS SELECT
    `project_name`
    ,`country_code`
    ,`platform`
    ,`device_type`
    ,`window_sig`
    ,`window`
    ,`base`
    ,`w1`
    ,`w2`
    ,`w3`
    ,`w4`
    ,`w5`
    ,`w6`
FROM dwb.dwb_fd_repurchase_report t
WHERE t.dt in (select max(dt) as max_dt from dwb.dwb_fd_repurchase_report);
