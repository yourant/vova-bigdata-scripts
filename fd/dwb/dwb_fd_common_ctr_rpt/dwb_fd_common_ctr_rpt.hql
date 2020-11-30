
insert overwrite table dwb.dwb_fd_common_ctr_rpt  partition(pt='${pt}')

SELECT     /*+ REPARTITION(1) */
          platform_type,
          app_version,
          country,
          `language`,
          project,
          page_code,
          element_events.absolute_position AS position,
          element_events.list_type AS list_name,
          element_events.element_name AS element_name,
          element_events.element_content AS element_content,
          element_events.element_type AS element_type,
          IF(event_name = 'common_impression', session_id, NULL) AS impression_session_id,
          IF(event_name = 'common_click', session_id, NULL) AS click_session_id
      from ods_fd_snowplow.ods_fd_snowplow_all_event LATERAL VIEW OUTER explode(element_event_struct) tmp as element_events
      where event_name in ('common_impression', 'common_click')
      and pt='${pt}';

