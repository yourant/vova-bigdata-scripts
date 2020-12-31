set hive.auto.convert.join=false;

insert overwrite table dwd.dwd_fd_place_selection_detail
    select
        /*+ REPARTITION(1) */
       gdoac.*,
       c.cat_name,
       vg.virtual_goods_id
    from
    ods_fd_vb.ods_fd_goods_display_order_artemis_country gdoac
    left join ods_fd_vb.ods_fd_goods g on gdoac.goods_id = g.goods_id
    left join ods_fd_vb.ods_fd_virtual_goods vg
      on gdoac.goods_id = vg.goods_id and lower(gdoac.project_name) = lower(vg.project_name)
    left join ods_fd_vb.ods_fd_category c
                                       on g.cat_id = c.cat_id;