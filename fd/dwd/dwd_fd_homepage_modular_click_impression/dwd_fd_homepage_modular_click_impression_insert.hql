set hive.new.job.grouping.set.cardinality = 256;
with base_temp as
(
    select
        dt,
        project,
        platform_type,
        country,
        is_new_user,
        app_version,
        session_id,
        modular,
        event_name
    from
    (
        (
            select
                to_date(collector_ts) dt,
                project,
                platform_type,
                if( country in ('FR', 'DE', 'SE', 'GB', 'AU', 'US', 'IT', 'ES', 'NL', 'MX', 'NO', 'AT', 'BE', 'CH', 'DK', 'CZ', 'PL', 'IL', 'BR', 'SA'),country,'others' ) country,
                case
                    when session_idx = 1 then 'new'
                    when session_idx > 1 then 'old'
                end as is_new_user, --新老用户
                app_version,
                session_id,
                if( platform_type = 'mobile_web' and element_event_struct.element_name = 'top_category',null,concat('banner_',element_event_struct.element_name,'_',element_event_struct.element_content)) as modular,

                event_name
            from
                ods_fd_snowplow.ods_fd_snowplow_element_event
            where
                pt = '${pt}'
                and platform_type in ('android_app','ios_app','mobile_web')
                and page_code = 'homepage'
                and element_event_struct.element_name in ('mainbanner','activitybanner','half_banner','multientrance_new','multientrance_banner',
                                                          'top_category','style_gallery','theme','oneplusthreebanner','keywordbanner','event_enterance','halfbanner','multientrance')
        )

        union all
        (
            select
                to_date(collector_ts) dt,
                project,
                platform_type,
                if( country in ('FR', 'DE', 'SE', 'GB', 'AU', 'US', 'IT', 'ES', 'NL', 'MX', 'NO', 'AT', 'BE', 'CH', 'DK', 'CZ', 'PL', 'IL', 'BR', 'SA'),country,'others' ) country,
                case
                    when session_idx = 1 then 'new'
                    when session_idx > 1 then 'old'
                end as is_new_user, --新老用户
                app_version,
                session_id,
                concat('product_',goods_event_struct.list_type) as modular,

                event_name
            from
                ods_fd_snowplow.ods_fd_snowplow_goods_event
            where
                pt = '${pt}'
                and platform_type in ('android_app','ios_app','mobile_web')
                and page_code = 'homepage'
                and goods_event_struct.list_type in ('top_picks','daily_recommanded','flash_sale','oneplusthree_banner','bestselling_new',
                                                     'home_clearance','style_gallery','recently_viewed','bestselling')
        )
    )
    where
        is_new_user is not null
)

INSERT OVERWRITE table dwd.dwd_fd_homepage_modular_click_impression partition (pt='${pt}')
select
	/*+ REPARTITION(1) */
    dt,
    project,
    platform_type,
    country,
    is_new_user,
    app_version,
    pv,
    uv,
    modular,

    impression_ss,
    click_ss,
    ctr,
    distinct_impression_ss,
    distinct_click_ss,
    uv_ctr
from
(
    select
        tab1.dt,
        tab1.project,
        tab1.platform_type,
        tab1.country,
        tab1.is_new_user,
        if(tab1.platform_type = 'mobile_web' and tab1.app_version = 'all','0',tab1.app_version) as app_version,
        pv,
        uv,
        tab1.modular,

        tab1.impression_ss,
        tab1.click_ss,
        concat( cast( round( if( tab1.impression_ss = 0,0,tab1.click_ss/tab1.impression_ss )*100,2 ) as string ),'%' ) ctr,
        tab1.distinct_impression_ss,
        tab1.distinct_click_ss,
        concat( cast( round( if( tab1.distinct_impression_ss = 0,0,tab1.distinct_click_ss/tab1.distinct_impression_ss )*100,2 ) as string ),'%' ) uv_ctr
    from
    (
        select
            dt,
            project,
            platform_type,
            country,
            is_new_user,
            app_version,
            modular,
            impression_ss,
            click_ss,
            distinct_impression_ss,
            distinct_click_ss
        from
        (
            select
                nvl(dt,'all') dt,
                nvl(project,'all') project,
                nvl(platform_type,'all') platform_type,
                nvl(country,'all') country,
                nvl(is_new_user,'all') is_new_user,
                nvl(app_version,'all') app_version,
                nvl(modular,'all') modular,

                count( if( event_name in ('common_impression','goods_impression'),session_id,null ) ) impression_ss,
                count( if( event_name in ( 'common_click','goods_click'),session_id,null ) ) click_ss,

                count( distinct if( event_name in ('common_impression','goods_impression'),session_id,null ) ) distinct_impression_ss,
                count( distinct if( event_name in ( 'common_click','goods_click'),session_id,null ) ) distinct_click_ss
            from
                base_temp
            group by
                dt,
                project,
                platform_type,
                country,
                is_new_user,
                app_version,
                modular with cube
        )
        where
            dt != 'all'
            and project != 'all'
            and modular != 'all'
    )tab1

    left join
    (
        select
            dt,
            project,
            platform_type,
            country,
            is_new_user,
            app_version,
            pv,
            uv
        from
        (
            select
                nvl(dt,'all') dt,
                nvl(project,'all') project,
                nvl(platform_type,'all') platform_type,
                nvl(country,'all') country,
                nvl(is_new_user,'all') is_new_user,
                nvl(app_version,'all') app_version,

                count( session_id ) pv,
                count( distinct session_id ) uv
            from
                base_temp
            where
                event_name in ('common_impression','goods_impression')
            group by
                dt,
                project,
                platform_type,
                country,
                is_new_user,
                app_version with cube
        )
        where
            dt != 'all'
            and project != 'all'
    )tab2
    on
        tab1.dt = tab2.dt
        and tab1.project = tab2.project
        and tab1.platform_type = tab2.platform_type
        and tab1.country = tab2.country
        and tab1.is_new_user = tab2.is_new_user
        and tab1.app_version = tab2.app_version
)
where
    app_version != '0'