set hive.new.job.grouping.set.cardinality = 256;
create table tmp.tmp_order_attribute_to_list_type as
select  nvl(tab1.project_name,'all') as project_name,
        nvl(tab1.country,'all') as country,
        nvl(tab1.platform_type,'all') as platform_type,
        nvl(tab1.page_code,'all') as page_code,
        nvl(tab1.list_type,'all') as list_type,
        count(tab1.goods_impression_session_id) as goods_impression_cnt, /* 曝光数 */
        count(tab1.goods_click_session_id) as goods_click_cnt, /* 点击数 */
        count(distinct tab1.goods_impression_domain_id) as goods_impression_uv_cnt, /* 曝光UV */
        count(distinct tab1.goods_click_domain_id) as goods_click_uv_cnt, /* 点击UV */
        count(distinct tab1.add_domain_id) as goods_add_uv_cnt, /* 加购成功UV */
        count(distinct tab1.order_id) as total_order_cnt, /* 订单 */
        count(distinct tab1.success_order_id) as total_success_order_cnt, /* 支付成功订单 */
        count(distinct tab1.success_order_user_id) as total_order_user_uv_cnt, /* 支付成功订单的用户 */
        sum(tab1.gmv) as gmv /* 订单金额 */
from (
    /* 曝光 点击 计算 */
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
    from tmp.tmp_fd_snowplow_click_impr
    where dt = '${hiveconf:dt}'  /* AND project_name = 'floryday' */
    AND project_name is not null
    AND country is not null
    AND country !=''
    AND page_code != '404'
    AND page_code != ''
    AND list_type != ''
    AND list_type is not null

    /* 加购计算 */
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
    from tmp.tmp_fd_snowplow_add_base
    where dt = '${hiveconf:dt}' /* AND project_name = 'floryday' */
    AND project_name is not null
    AND country is not null
    AND country !=''
    AND page_code != '404'
    AND page_code != ''
    AND page_code is not null
    AND list_type != ''
    AND list_type is not null
    AND list_type is not null

    /* 订单数据 */
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
        if(pay_status = 9,order_id,null) as order_id, /* 订单 */
        if(pay_status = 2,order_id,null) as success_order_id, /* 支付成功订单 */
        if(pay_status = 2,user_id,null) as success_order_user_id, /* 支付成功订单的用户 */
        if(pay_status = 2,order_amount,null) as gmv /* 订单金额包括运费 */
    from tmp.tmp_fd_snowplow_order_base
    where dt = '${hiveconf:dt}'
    /* AND project_name = 'floryday' */
    AND project_name is not null
    AND country is not null
    AND country !=''
    AND page_code != '404'
    AND page_code != ''
    AND list_type != ''
    AND list_type is not null

) tab1 group by tab1.project_name,tab1.country,tab1.platform_type,tab1.page_code,tab1.list_type with cube
having sum(tab1.gmv) > 0;


