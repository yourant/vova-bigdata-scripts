create table if not exists dwd.dwd_fd_session_channel_arc
(
    `session_id`    string,
    `ga_channel`    string,
    `mkt_source`    string,
    `campaign_name` string,
    `campaign_id`   string,
    `adgroup_id`    string,
    `mkt_medium`    string,
    `mkt_term`      string,
    `domain_userid` string,
    `derived_ts`    string
)
    partitioned by (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;