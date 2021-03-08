insert overwrite table dwb.dwb_fd_goods_test_channel
select
     /*+ REPARTITION(1) */
    nvl(project,'all') as project,
    nvl(cat_name,'all') as cat_name,
    nvl(end_day,'all') as end_day,
    nvl(selection_mode,'all') as selection_mode,
    nvl(selection_channel,'all') as selection_channel,
    nvl(channel_type,'all') as channel_type,
    virtual_goods_id,
    result as test_result,

    sum(last_7_days_goods_sales) as last_7_days_goods_sales,
    sum(last_7_days_cat_sales) as last_7_days_cat_sales,
    nvl(sum(last_7_days_goods_sales) / sum(last_7_days_cat_sales),0) as sale_rate

from (
    -- 测款成功的商品
    select
        t2.project,
        t2.cat_name,
        t2.end_day,
        t2.selection_mode,
        t2.selection_channel,
        t2.channel_type,
        t2.virtual_goods_id,
        1 as result,
        sum(nvl(t3.last_7_days_goods_sales,0)) as last_7_days_goods_sales,
        max(nvl(t4.last_7_days_cat_sales,0)) as last_7_days_cat_sales
    from (

        select
            goods_id,
            virtual_goods_id,
            result,
            project,
            cat_id,
            cat_name,
            case
                when selection_mode in ('贸综挑款','营销测款','运营爬虫测款','运营属性测款','运营中台选款') then selection_mode
            else '其他' end as selection_mode,
            case
                when selection_channel in ('other','') then '其他'
                else selection_channel end selection_channel,
            channel_type,
            to_date(to_utc_timestamp(end_time, 'America/Los_Angeles')) as end_day --测款结束时间

        from dwd.dwd_fd_goods_test_goods_detail
        where result = 1
          and end_time is not null
          and virtual_goods_id is not null
          and lower(project) in ('airydress','floryday')
          and cat_id is not null
          and cat_name is not null

    ) t2
 -- 从商品订单信息表中获取商品近7天的销售额（维度：项目，商品id）
 -- 这里计算到的是测款成功的商品的销售额度,每一个商品的销售额度
    left join (

        select
            project_name,
            goods_id,
            sum(goods_number * shop_price) as last_7_days_goods_sales
        from dwd.dwd_fd_order_goods
        where pay_status = 2 and project_name in ('airydress','floryday')
        and from_unixtime(pay_time) between date_sub('${pt}',6) and '${pt}'
        group by project_name,goods_id

    ) t3 on t2.goods_id = t3.goods_id and t2.project = t3.project_name
 --从商品订单信息表中获取不同项目下不同品类的近7天的销售额（维度：项目，品类）
 --这里计算的是同品类下的所有商品的销售额度，包含：测款的商品和非测款的商品
    left join (

        select
            project_name,
            cat_id,
            sum(goods_number * shop_price) as last_7_days_cat_sales
        from dwd.dwd_fd_order_goods
        where pay_status = 2 and project_name in ('airydress','floryday')
        and from_unixtime(pay_time) between date_sub('${pt}',6) and '${pt}'
        group by project_name, cat_id

    ) t4 on t2.cat_id = t4.cat_id and t2.project = t4.project_name

    group by t2.project,t2.cat_name,t2.end_day,t2.selection_mode,t2.selection_channel,t2.channel_type,t2.virtual_goods_id

    -- 测款失败的商品
    union all
    select
        project,
        cat_name,
        end_day,
        selection_mode,
        selection_channel,
        channel_type,
        virtual_goods_id,
        0 as result, --测试失败
        0 as last_7_days_goods_sales,
        0 as last_7_days_cat_sales
    from (

        select
            goods_id,
            virtual_goods_id,
            result,
            project,
            cat_id,
            cat_name,
            case
                when selection_mode in ('贸综挑款','营销测款','运营爬虫测款','运营属性测款','运营中台选款') then selection_mode
            else '其他' end as selection_mode,
            case
                when selection_channel in ('other','') then '其他'
                else selection_channel end selection_channel,
            channel_type,
            to_date(to_utc_timestamp(end_time, 'America/Los_Angeles')) as end_day --测款结束时间

        from dwd.dwd_fd_goods_test_goods_detail
        where result = 2
          and end_time is not null
          and virtual_goods_id is not null
          and lower(project) in ('airydress','floryday')
          and cat_id is not null
          and cat_name is not null

    ) t2
    group by project,cat_name,end_day,selection_mode,selection_channel,channel_type,virtual_goods_id

) t1
group by result, virtual_goods_id, project, cat_name, end_day, selection_mode, selection_channel, channel_type with cube;
