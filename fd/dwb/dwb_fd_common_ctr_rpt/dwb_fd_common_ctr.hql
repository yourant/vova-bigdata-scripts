use dwb;


CREATE VIEW IF NOT EXISTS dwb.dwb_fd_rpt_common_ctr AS

SELECT 
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
    dt
from ods.ods_fd_snowplow_element_event
where event_name in ('common_impression', 'common_click');

