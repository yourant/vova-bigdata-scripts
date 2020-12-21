
drop table if exists tmp_place_selection;
set hive.auto.convert.join=false;

create table if not exists tmp.tmp_place_selection as
    select
       gdoac.*,
       c.cat_name,
       vg.virtual_goods_id
    from
    ods_fd_vb.ods_fd_goods_display_order_artemis_country gdoac
    left join ods_fd_vb.ods_fd_goods g on gdoac.goods_id = g.goods_id
    left join ods_fd_vb.ods_fd_virtual_goods vg
                                       on gdoac.goods_id = vg.goods_id and gdoac.project_name = vg.project_name
    left join ods_fd_vb.ods_fd_category c
                                       on g.cat_id = c.cat_id;