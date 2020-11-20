CREATE TABLE IF NOT EXISTS dwb.dwb_fd_newsletter_send_rpt (
`project` string COMMENT '组织',
`nl_code_num` string COMMENT 'nl期数',
`nl_code` string COMMENT 'nl_code',
`nl_type` string COMMENT 'nl_type',
`create_time` string COMMENT '创建时间',
`send_time` string COMMENT '发送时间',
`total_count` bigint COMMENT '发送量',
`success_count` bigint COMMENT '成功量',
`fail_count` bigint COMMENT '失败量',
`open_count` bigint COMMENT '打开量',
`unsubscribe_count` bigint COMMENT '退订量'
) COMMENT 'Newsltter 发送量报表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
insert overwrite table dwb.dwb_fd_newsletter_send_rpt partition (pt)
select tab1.project,
       tab1.nl_code_num,
       tab1.nl_code,
       tab1.nl_type,
       tab1.create_time,
       tab1.send_time,
       sum(tab1.total_count),
       sum(tab1.success_count),
       sum(tab1.fail_count),
       sum(tab1.open_count),
       sum(0) as unsubscribe_count,
       tab1.pt as pt
from (
      select
            case
                when lower(substr(nl_code,1,2)) = 'ad' then 'airydress'
                when lower(substr(nl_code,1,2)) = 'fd' then 'floryday'
                when lower(substr(nl_code,1,2)) = 'sd' then 'sisdress'
                when lower(substr(nl_code,1,2)) = 'td' then 'tendaisy'
            end as project,
          split(nl_code,'_')[0] as nl_code_num,
          nl_code,
          nl_type,
          TO_UTC_TIMESTAMP(create_time, 'America/Los_Angeles') as create_time,
          TO_UTC_TIMESTAMP(send_time, 'America/Los_Angeles') as send_time,
          send_count as total_count,
          arrive_count as success_count,
          (send_count-arrive_count) as fail_count,
          open_count,
          0 as unsubscribe_count,
          date(TO_UTC_TIMESTAMP(send_time, 'America/Los_Angeles')) as pt
    from ods_fd_nl.ods_fd_newsletters
    where date(TO_UTC_TIMESTAMP(send_time, 'America/Los_Angeles')) >= date_sub('${hiveconf:pt}',15)
    and date(TO_UTC_TIMESTAMP(send_time, 'America/Los_Angeles')) <= '${hiveconf:pt}'
    and lower(substr(nl_code,1,2)) IN ('ad','fd','sd','td')
) tab1
group by tab1.project,
         tab1.nl_code_num,
         tab1.nl_code,
         tab1.nl_type,
         tab1.create_time,
         tab1.send_time,
         tab1.pt;
