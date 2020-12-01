insert overwrite table dwd.dwd_fd_snowplow_add partition (pt = '${pt}')
SELECT /*+ REPARTITION(2) */
    tab1.project_name,
    tab1.country,
    tab1.platform_type,
    tab1.event_name,
    tab1.domain_userid,
    if(tab2.domain_userid is not null,tab2.page_code,tab3.page_code) as page_code,
    if(tab2.domain_userid is not null,tab2.list_type,tab3.list_type) as list_type
from (
    SELECT
        project AS project_name, /* 组织 */
        upper(country) as country, /* 国家 */
        platform_type, /* 平台 */
        domain_userid,
        event_name,
        page_code,
        session_id
    FROM ods_fd_snowplow.ods_fd_snowplow_ecommerce_event
    WHERE event_name in ('add')
    AND pt = '${pt}'
    AND project is not null
    AND project != ''
    AND length(country) = 2
) tab1
left join (
    SELECT t0.project_name,t0.country,t0.platform_type,t0.page_code,t0.list_type,t0.domain_userid
    FROM (
        SELECT
            project AS project_name,
            upper(country) as country,
            platform_type,
            page_code,
            goods_event_struct.list_type,
            domain_userid,
            row_number() over (partition by project,country,platform_type,platform_type,domain_userid order by derived_tstamp desc) as rn
        FROM ods_fd_snowplow.ods_fd_snowplow_goods_event
        WHERE pt = '${pt}'
        AND event_name = 'goods_click'
        AND project is not null
        AND project != ''
        AND length(country) = 2
        AND platform_type is not null
        AND platform_type != ''
        AND page_code != '404'
        AND page_code != ''
        AND goods_event_struct.list_type is not null
        AND goods_event_struct.list_type != 'null'
        AND goods_event_struct.list_type != 'NULL'
        AND goods_event_struct.list_type != ''
    ) t0 WHERE t0.rn = 1

) tab2 on tab1.project_name = tab2.project_name AND tab1.country = tab2.country AND tab1.platform_type = tab2.platform_type AND tab1.domain_userid = tab2.domain_userid
left join (
    SELECT t0.project_name,t0.country,t0.platform_type,t0.page_code,t0.list_type,t0.domain_userid
    FROM (
          SELECT
                project AS project_name,
                upper(country) as country,
                platform_type,
                page_code,
                goods_event_struct.list_type,
                domain_userid,
                row_number() over (partition by project,country,platform_type,platform_type,domain_userid order by derived_tstamp desc) as rn
            FROM ods_fd_snowplow.ods_fd_snowplow_goods_event
            WHERE pt = '${pt}'
            AND event_name = 'goods_impression'
            AND project is not null
            AND project != ''
            AND length(country) = 2
            AND platform_type is not null
            AND platform_type != ''
            AND page_code != '404'
            AND page_code != ''
            AND goods_event_struct.list_type is not null
            AND goods_event_struct.list_type != 'null'
            AND goods_event_struct.list_type != 'NULL'
            AND goods_event_struct.list_type != ''
    ) t0 WHERE t0.rn = 1

) tab3 on tab1.project_name = tab3.project_name AND tab1.country = tab3.country AND tab1.platform_type = tab3.platform_type AND tab1.domain_userid = tab3.domain_userid;
