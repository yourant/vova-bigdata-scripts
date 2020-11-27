
insert overwrite table dwb.dwb_fd_common_ctr_rpt  partition(pt='${pt}')
SELECT
    platform_type,
    app_version,
    country,
    `language`,
    project,
    page_code,
    nvl(position,''),
    nvl(list_name,'unknown'),
    nvl(element_name,'unknown'),
    nvl(element_content,'unknown'),
    nvl(element_type,'unknown'),
    count(DISTINCT impression_session_id),
    count(DISTINCT click_session_id)
from (SELECT
          platform_type,
          app_version,
          country,
          `language`,
          project,
          page_code,
          element_event_struct.absolute_position AS position,
          element_event_struct.list_type AS list_name,
          element_event_struct.element_name AS element_name,
          element_event_struct.element_content AS element_content,
          element_event_struct.element_type AS element_type,
          IF(event_name = 'common_impression', session_id, NULL) AS impression_session_id,
          IF(event_name = 'common_click', session_id, NULL) AS click_session_id,
          pt
      from ods_fd_snowplow.ods_fd_snowplow_all_event
      where event_name in ('common_impression', 'common_click')
      and pt='${pt}'
      )tab1

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

