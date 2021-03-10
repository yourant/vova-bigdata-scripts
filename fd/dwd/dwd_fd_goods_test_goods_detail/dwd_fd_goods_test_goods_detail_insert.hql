insert overwrite table dwd.dwd_fd_goods_test_goods_detail
select
    /*+ REPARTITION(1) */
    gtg.goods_id, --商品id
    g.goods_name,
    g.virtual_goods_id, --虚拟商品id
    g.cat_id, --品类ID
    g.cat_name, --品类名
    g.goods_selector, --选款人
    ge.selection_mode, --选款方式
    trim(ge.selection_channel) as selection_channel, --选款渠道
    case
    	when gtsc.source_channel is null then '非中台爬虫渠道'
    	else '中台爬虫渠道' end as channel_type, --渠道类型
    gtg.project_name as project, --组织
    gtg.result, --测款结果
    gtg.reason,--测款理由
    gtg.type_name, -- 商品类型：贸综/正常
    gtg.create_time,--测款添加时间
    gtg.test_time,-- 入测时间
    gtg.end_time, --测款结束时间
    gtg.last_update_time --最后更新时间
from ods_fd_vb.ods_fd_goods_test_goods_report gtg
left join dim.dim_fd_goods g on gtg.goods_id = g.goods_id and lower(gtg.project_name) = lower(g.project_name)
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
        group by goods_id
    ) t1
    where ext_name NOT REGEXP 'original_selection_name|original_source_name'

    union all

    select
        goods_id,
        case
            when name_list[0] = 'original_selection_name' then value_list[0]
            when name_list[1] = 'original_selection_name' then value_list[1]
            else 'other' end as selection_mode,
        case
            when name_list[0] = 'original_source_name' then value_list[0]
            when name_list[1] = 'original_source_name' then value_list[1]
            else 'other' end as selection_channel
    from (
        select
            goods_id,
            collect_list(ext_name) as name_list,
            collect_list(ext_value) as value_list
        from ods_fd_vb.ods_fd_goods_extension
        where ext_name in ('original_source_name','original_selection_name')
        group by goods_id
    ) t2

) ge on gtg.goods_id = ge.goods_id
left join ods_fd_vb.ods_fd_goods_test_source_channel gtsc on trim(ge.selection_channel) = trim(gtsc.source_channel);