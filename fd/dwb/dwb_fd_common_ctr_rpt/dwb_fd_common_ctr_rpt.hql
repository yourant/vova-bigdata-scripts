set hive.new.job.grouping.set.cardinality=256;
insert overwrite table dwb.dwb_fd_common_ctr_rpt  partition(pt='${pt}')
SELECT
    /*+ REPARTITION(1) */
    nvl(platform_type,'all') as platform_type,
    nvl(app_version,'all') as app_version,
    nvl(country,'all') as country,
    nvl(language,'all') as language,
    nvl(project,'all') as project,
    nvl(page_code,'all') as page_code,
    nvl(position,'all') as position,
    nvl(list_name,'all') as list_name,
    nvl(element_name,'all') as element_name,
    nvl(element_content,'all') as element_content,
    nvl(element_type,'all') as element_type,
    count(DISTINCT impression_session_id),
    count(DISTINCT click_session_id)
from (SELECT
          nvl(platform_type,'other') as platform_type,
          nvl(app_version,'other') as app_version,
          nvl(country,'other') as country,
          nvl(`language`,'other') as language,
          nvl(project,'other') as project,
          nvl(page_code,'other')  as page_code,
          cast(element_event_struct.absolute_position  as string) AS position,
          element_event_struct.list_type AS list_name,
          element_event_struct.element_name AS element_name,
          element_event_struct.element_content AS element_content,
          element_event_struct.element_type AS element_type,
          IF(event_name = 'common_impression', session_id, NULL) AS impression_session_id,
          IF(event_name = 'common_click', session_id, NULL) AS click_session_id,
          pt
      from ods_fd_snowplow.ods_fd_snowplow_all_event
      where event_name in ('common_impression', 'common_click')
      and country is not null
      and pt='${pt}'
      )tab1
GROUP by platform_type,
	app_version,
	country,
	language,
	project,
	page_code,
	position,
	list_name,
	element_name,
	element_content,
	element_type with cube;

