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
    sum(last_7_days_cat_sales) as last_7_days_cat_sales
from (
    select
        t2.project,
        t2.cat_name,
        t2.end_day,
        t2.selection_mode,
        t2.selection_channel,
        t2.channel_type,
        count(*) as success_num,
        sum(last_7_days_goods_sales) as last_7_days_goods_sales,
        sum(last_7_days_cat_sales) as last_7_days_cat_sales,
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
                selection_mode,
                selection_channel,
                channel_type,
                date_format(end_time,'yyyy-MM-dd') as end_day,
                row_number() over(partition by goods_id order by end_time) as rn
            from dwd.dwd_fd_goods_test_detail
            where test_type = '1' and result = '1'
        ) t1
        where rn = 1
    ) t2
    left join (
        select
            goods_id,
            cat_id,
            project_name,
            sum(goods_sales) over(partition by project_name,goods_id) as last_7_days_goods_sales,
            sum(goods_sales) over(partition by project_name,cat_id) as last_7_days_cat_sales
        from (
            select
                goods_id,
                cat_id,
                project_name,
                sum(goods_number * shop_price) as goods_sales
            from dwd.dwd_fd_order_goods
            where pay_status = 2
            and from_unixtime(pay_time) between date_add('${pt}',-6) and '${pt}'
            group by
                goods_id,
                cat_id,
                project_name
        ) t3
    ) t4 on t2.goods_id = t4.goods_id and t2.cat_id = t4.cat_id and t2.project = t4.project_name
    where t4.goods_id is not null
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
        project,
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
                selection_mode,
                selection_channel,
                channel_type,
                date_format(end_time,'yyyy-MM-dd') as end_day,
                row_number() over(partition by goods_id order by end_time desc) as rn
            from dwd.dwd_fd_goods_test_detail
            where test_type = '1' and result not in ('1','0','8')
        ) t1
        where rn = 1
    ) t2
    left join (
        select
            goods_id
        from dwd.dwd_fd_goods_test_detail
        where test_type = '1' and result = '1'
        group by
            goods_id
    ) t3 on t2.goods_id = t3.goods_id
    where t3.goods_id is null
    group by
        project,
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
