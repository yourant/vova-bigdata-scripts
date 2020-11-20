use dwb;

CREATE table if not exists  dwb.dwb_fd_common_ctr_rpt
(
    platform_type string,
    app_version string,
    country string,
    `language` string,
    project string,
    page_code string,
    position string,
    list_name string,
    element_name string,
    element_content string,
    element_type string,
    impression_uv bigint,
    click_uv  bigint
)comment '打点数据common_event的ctr报表'
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc
TBLPROPERTIES ("orc.compress"="SNAPPY");

insert overwrite table dwb.dwb_fd_common_ctr_rpt  partition(dt='${hiveconf:dt}')
SELECT
    platform_type,
    app_version,
    country,
    `language`,
    project,
    page_code,
    position,
    list_name,
    element_name,
    element_content,
    element_type,
    count(DISTINCT impression_session_id),
    count(DISTINCT click_session_id)
from dwb.dwb_fd_rpt_common_ctr
where dt='${hiveconf:dt}'
GROUP by platform_type,
	app_version,
	country,
	`language`,
	project,
	page_code,
	position,
	list_name,
	element_name,
	element_content,
	element_type ;

