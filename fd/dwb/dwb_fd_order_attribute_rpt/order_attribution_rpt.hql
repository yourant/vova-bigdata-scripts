set hive.new.job.grouping.set.cardinality = 256;
insert overwrite table dwb.dwb_fd_order_attribution_rpt partition (pt = '${pt}')
select  /*+ REPARTITION(1) */
        nvl(tab1.project_name,'all') as project_name,
        nvl(tab1.country,'all') as country,
        nvl(tab1.platform_type,'all') as platform_type,
        nvl(tab1.page_code,'all') as page_code,
        nvl(tab1.list_type,'all') as list_type,
        count(tab1.goods_impression_session_id) as goods_impression_cnt,
        count(tab1.goods_click_session_id) as goods_click_cnt,
        count(distinct tab1.goods_impression_domain_id) as goods_impression_uv_cnt,
        count(distinct tab1.goods_click_domain_id) as goods_click_uv_cnt,
        count(distinct tab1.add_domain_id) as goods_add_uv_cnt,
        count(distinct tab1.order_id) as total_order_cnt,
        count(distinct tab1.success_order_id) as total_success_order_cnt,
        count(distinct tab1.success_order_user_id) as total_order_user_uv_cnt,
        sum(tab1.gmv) as gmv
from (
    select
        project_name,
        upper(country) as country,
        platform_type,
        page_code,
        list_type,
        if(event_name = 'goods_impression', session_id, NULL) AS goods_impression_session_id,
        if(event_name = 'goods_click', session_id, NULL) AS goods_click_session_id,
        if(event_name = 'goods_impression', domain_userid, NULL) AS goods_impression_domain_id,
        if(event_name = 'goods_click', domain_userid, NULL) AS goods_click_domain_id,
        null as add_domain_id,
        null as order_id,
        null as success_order_id,
        null as success_order_user_id,
        null as gmv
    from dwd.dwd_fd_snowplow_click_impr
    where pt = '${pt}'
    AND project_name is not null
    AND country is not null
    AND country !=''
    AND page_code != '404'
    AND page_code != ''
    AND list_type != ''
    AND list_type is not null

    union all
    select
        project_name,
        upper(country) as country,
        platform_type,
        page_code,
        list_type,
        null as goods_impression_session_id,
        null as goods_click_session_id,
        null as goods_impression_domain_id,
        null as goods_click_domain_id,
        if(event_name = 'add', domain_userid, NULL) as add_domain_id,
        null as order_id,
        null as success_order_id,
        null as success_order_user_id,
        null as gmv
    from dwd.dwd_fd_snowplow_add
    where pt = '${pt}'
    AND project_name is not null
    AND country is not null
    AND country !=''
    AND page_code != '404'
    AND page_code != ''
    AND page_code is not null
    AND list_type != ''
    AND list_type is not null
    AND list_type is not null

    union all
    select
        project_name,
        upper(country) as country,
        platform_type,
        page_code,
        list_type,
        null as goods_impression_session_id,
        null as goods_click_session_id,
        null as goods_impression_domain_id,
        null as goods_click_domain_id,
        null as add_domain_id,
        if(pay_status = '9',order_id,null) as order_id,
        if(pay_status = '2',order_id,null) as success_order_id,
        if(pay_status = '2',user_id,null) as success_order_user_id,
        if(pay_status = '2',order_amount,null) as gmv
    from dwd.dwd_fd_snowplow_order
    where pt = '${pt}'
    AND project_name is not null
    AND country is not null
    AND country !=''
    AND page_code != '404'
    AND page_code != ''
    AND list_type != ''
    AND list_type is not null

) tab1 group by tab1.project_name,tab1.country,tab1.platform_type,tab1.page_code,tab1.list_type with cube
having sum(tab1.gmv) > 0;
