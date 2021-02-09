insert overwrite table dwd.dwd_fd_goods_test_detail
select
    /*+ REPARTITION(1) */
    gtg.goods_id,
    g.goods_name,
    g.virtual_goods_id,
    g.cat_id,
    g.cat_name,
    g.goods_selector,
    t3.selection_mode,
    trim(t3.selection_channel) as selection_channel,
    case when gtsc.source_channel is null then '非中台爬虫渠道' else '中台爬虫渠道' end as channel_type,
    gtp.project,
    gtp.country,
    gtp.platform,
    gtp.pipeline_id,
    gtg.state,
    gtg.type_id,
    gtg.result,
    gtg.reason,
    gtg.production_reached,
    gtg.goods_type,
    gtg.goods_source,
    gtg.test_count,
    gtg.test_type,
    gtg.admin_name,
    gtg.is_auto,
    gtg.type_name,
    gtg.create_time,
    gtg.test_time,
    gtg.end_time,
    gtg.last_update_time
from ods_fd_vb.ods_fd_goods_test_goods gtg
left join ods_fd_vb.ods_fd_goods_test_pipeline gtp on gtg.pipeline_id = gtp.pipeline_id
left join dim.dim_fd_goods g on gtg.goods_id = g.goods_id and gtp.project = g.project_name
left join (
    select
        goods_id,
        'other' as selection_mode,
        'other' as selection_channel
    from (
        select
            goods_id,
            concat_ws(',',collect_list(ext_name)) as ext_name
        from ods_fd_vb.ods_fd_goods_extension
        group by
            goods_id
    ) t1
    where ext_name NOT REGEXP 'original_selection_name|original_source_name'

    union all

    select
        goods_id,
        case
            when name_list[0] = 'original_selection_name' then value_list[0]
            when name_list[1] = 'original_selection_name' then value_list[1]
            else 'other'
        end as selection_mode,
        case
            when name_list[0] = 'original_source_name' then value_list[0]
            when name_list[1] = 'original_source_name' then value_list[1]
            else 'other'
        end as selection_channel
    from (
        select
            goods_id,
            collect_list(ext_name) as name_list,
            collect_list(ext_value) as value_list
        from ods_fd_vb.ods_fd_goods_extension
        where ext_name in ('original_source_name','original_selection_name')
        group by
            goods_id
    ) t2
) t3 on gtg.goods_id = t3.goods_id
left join ods_fd_vb.ods_fd_goods_test_source_channel gtsc on trim(t3.selection_channel) = trim(gtsc.source_channel)
