select
                            gdoac.project_name,
                            gdoac.goods_id,
                            vg.virtual_goods_id,
                            country_code,
                            c.cat_name,
                            platform,
                            sum(impressions) as impressions,
                            sum(sales_order) as sales_order,
                            sum(clicks) as clicks,
                            sum(users) as users,
                            (sum(clicks) / sum(impressions)) * 100                           as ctr,
                            (sum(clicks) / sum(impressions)) * (sum(sales_order) / sum(users)) * 10000 as cr,
                            (sum(detail_add_cart) / sum(users))*100  as add_rate,
                            (sum(checkout) / sum(users))*100  as kr,
                            (sum(sales_order) / sum(users))*100  as rate,

                     from ods_fd_vb.ods_fd_goods_display_order_artemis_country gdoac
                              left join ods_fd_vb.ods_fd_goods g on gdoac.goods_id = g.goods_id
                              left join ods_fd_vb.ods_fd_virtual_goods vg
                                        on gdoac.goods_id = vg.goods_id and gdoac.project_name = vg.project_name
                              left join ods_fd_vb.ods_fd_category c on g.cat_id = c.cat_id
                              GROUP by              gdoac.project_name,
                                                    gdoac.goods_id,
                                                    vg.virtual_goods_id,
                                                    country_code,
                                                    c.cat_name,
                                                    platform
