insert overwrite table dwb.dwb_fd_goods_test_channel_rpt
select
     /*+ REPARTITION(1) */
    nvl(project,'all') as project,
    nvl(cat_name,'all') as cat_name,
    nvl(end_day,'all') as end_day,
    nvl(selection_mode,'all') as selection_mode,
    nvl(selection_channel,'all') as selection_channel,
    nvl(channel_type,'all') as channel_type,
    sum(success_num) as success_num,
    sum(fail_num) as fail_num,
    sum(last_7_days_goods_sales) as last_7_days_goods_sales,
    sum(last_7_days_cat_sales) as last_7_days_cat_sales,
    sum(last_7_days_goods_sales) / sum(last_7_days_cat_sales) as sale_rate
from (
    select
        t2.project,
        t2.cat_name,
        t2.end_day,
        t2.selection_mode,
        t2.selection_channel,
        t2.channel_type,
        count(*) as success_num,
        sum(nvl(last_7_days_goods_sales,0)) as last_7_days_goods_sales,
        if(t2.project is null,sum(nvl(last_7_days_cat_sales,0)),max(nvl(last_7_days_cat_sales,0))) as last_7_days_cat_sales,
        0 as fail_num
    from (
        select
            goods_id,
            project,
            cat_id,
            cat_name,
            selection_mode,
            selection_channel,
            channel_type,
            end_day
        from (
            select
                goods_id,
                project,
                cat_id,
                cat_name,
                case when selection_mode in ('贸综挑款','营销测款','运营爬虫测款','运营属性测款','运营中台选款') then selection_mode else '其他' end as selection_mode,
                case when selection_channel in ('other','') then '其他' else selection_channel end selection_channel,
                channel_type,
                date_format(end_time,'yyyy-MM-dd') as end_day,
                row_number() over(partition by project,goods_id order by end_time) as rn
            from dwd.dwd_fd_goods_test_detail
            where test_type = '1' and result = '1'
        ) t1
        where rn = 1
    ) t2
    left join (
    select
        project_name,
        goods_id,
        sum(goods_number * shop_price) as last_7_days_goods_sales
    from dwd.dwd_fd_order_goods
    where pay_status = 2
    and from_unixtime(pay_time) between date_add('${pt}',-6) and '${pt}'
    group by
        project_name,
        goods_id
    ) t3 on t2.goods_id = t3.goods_id and t2.project = t3.project_name
    left join (
    select
        project_name,
        cat_id,
        sum(goods_number * shop_price) as last_7_days_cat_sales
    from dwd.dwd_fd_order_goods
    where pay_status = 2
    and from_unixtime(pay_time) between date_add('${pt}',-6) and '${pt}'
    group by
        project_name,
        cat_id
    ) t4 on t2.cat_id = t4.cat_id and t2.project = t4.project_name
    group by
        t2.project,
        t2.cat_name,
        t2.end_day,
        t2.selection_mode,
        t2.selection_channel,
        t2.channel_type
    with cube

    union all

    select
        t2.project,
        cat_name,
        end_day,
        selection_mode,
        selection_channel,
        channel_type,
        0 as success_num,
        0 as last_7_days_goods_sales,
        0 as last_7_days_cat_sales,
        count(*) as fail_num
    from (
        select
            goods_id,
            project,
            cat_name,
            end_day,
            selection_mode,
            selection_channel,
            channel_type
        from (
            select
                goods_id,
                project,
                cat_name,
                case when selection_mode in ('贸综挑款','营销测款','运营爬虫测款','运营属性测款','运营中台选款') then selection_mode else '其他' end as selection_mode,
                case when selection_channel in ('other','') then '其他' else selection_channel end selection_channel,
                channel_type,
                date_format(end_time,'yyyy-MM-dd') as end_day,
                row_number() over(partition by project,goods_id order by end_time desc) as rn
            from dwd.dwd_fd_goods_test_detail
            where test_type = '1' and result not in ('1','0','8')
        ) t1
        where rn = 1
    ) t2
    left join (
        select
            project,
            goods_id
        from dwd.dwd_fd_goods_test_detail
        where test_type = '1' and result = '1'
        group by
            project,
            goods_id
    ) t3 on t2.goods_id = t3.goods_id and t2.project = t3.project
    where t3.goods_id is null
    group by
        t2.project,
        cat_name,
        end_day,
        selection_mode,
        selection_channel,
        channel_type
    with cube
) t1
group by
    project,
    cat_name,
    end_day,
    selection_mode,
    selection_channel,
    channel_type
;