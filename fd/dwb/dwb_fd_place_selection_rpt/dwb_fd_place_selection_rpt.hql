
CREATE TABLE if not exists dwb.dwb_fd_place_selection_rpt (
  `data_type` string COMMENT '数据类型',
  `project_name` string COMMENT '网站组织名',

  `goods_id` bigint  COMMENT '商品真实id',
  `virtual_goods_id` bigint COMMENT '商品虚拟id',

  `country_code` string COMMENT '国家',
  `cat_name` string COMMENT '品类名称',
  `platform` string COMMENT '网站平台(web/h5/mob)',
  `impressions` bigint COMMENT '页面展示数',
  `sales_order` bigint COMMENT '商品销售件数',
  `clicks` bigint COMMENT '页面点击数',
  `users` bigint COMMENT '访问用户数',
  `ctr` DECIMAL(15, 4) COMMENT 'ctr',
  `cr` DECIMAL(15, 4) COMMENT 'cr',
  `country_cat_platform_top` bigint COMMENT '国家+品类+设备粒度cr排名',
  `cat_platform_top` bigint COMMENT '品类+设备粒度cr排名'

) COMMENT'投放选款报表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC;

insert overwrite table dwb.dwb_fd_place_selection_rpt
select
       'sales',
       'floryday',
       goods_id,
       virtual_goods_id,
       country_code,
       cat_name,
       platform,
       impressions,
       sales_order,
       clicks,
       users,
       ctr,
       cr,
       rn,
       rn1
from (select goods_id,
             virtual_goods_id,
             country_code,
             cat_name,
             platform,
             impressions,
             sales_order,
             clicks,
             users,
             ctr,
             cr,
             rn,
             row_number() over (partition by cat_name,platform order by cr desc) rn1
      from (
               select goods_id,
                      virtual_goods_id,
                      country_code,
                      cat_name,
                      platform,
                      impressions,
                      sales_order,
                      clicks,
                      users,
                      ctr,
                      cr,
                      row_number() over (partition by country_code,cat_name,platform order by cr desc ) rn
               from (select 
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
                            (sum(clicks) / sum(impressions)) * (sum(sales_order) / sum(users)) * 10000 as cr
                     from tmp.tmp_fd_goods_display_order_artemis_country_full gdoac
                              left join tmp.tmp_fd_goods_full g on gdoac.goods_id = g.goods_id
                              left join tmp.tmp_fd_virtual_goods_full vg
                                        on gdoac.goods_id = vg.goods_id and gdoac.project_name = vg.project_name
                              left join tmp.tmp_fd_category_full c on g.cat_id = c.cat_id
                              GROUP by              gdoac.project_name,
                                                    gdoac.goods_id,
                                                    vg.virtual_goods_id,
                                                    country_code,
                                                    c.cat_name,
                                                    platform
                    ) t1
               where project_name = 'floryday'
                 and impressions > 1000
                 and sales_order >= 100
                 and users > 10
                 and cr > 10
                 and ctr > 1
                 and users > clicks
           ) t2
      where rn <= 4
     ) t3
where rn1 <= 8

union all

select
       'potential',
       'floryday',
       goods_id,
       virtual_goods_id,
       country_code,
       cat_name,
       platform,
       impressions,
       sales_order,
       clicks,
       users,
       ctr,
       cr,
       rn,
       rn1
from (select goods_id,
             virtual_goods_id,
             country_code,
             cat_name,
             platform,
             impressions,
             sales_order,
             clicks,
             users,
             ctr,
             cr,
             rn,
             row_number() over (partition by cat_name,platform order by cr desc) rn1
      from (
               select goods_id,
                      virtual_goods_id,
                      country_code,
                      cat_name,
                      platform,
                      impressions,
                      sales_order,
                      clicks,
                      users,
                      ctr,
                      cr,
                      row_number() over (partition by country_code,cat_name,platform order by cr desc ) rn
               from (select 
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
                            (sum(clicks) / sum(impressions)) * (sum(sales_order) / sum(users)) * 10000 as cr
                     from tmp.tmp_fd_goods_display_order_artemis_country_full gdoac
                              left join tmp.tmp_fd_goods_full g on gdoac.goods_id = g.goods_id
                              left join tmp.tmp_fd_virtual_goods_full vg
                                        on gdoac.goods_id = vg.goods_id and gdoac.project_name = vg.project_name
                              left join tmp.tmp_fd_category_full c on g.cat_id = c.cat_id
                              GROUP by              gdoac.project_name,
                                                    gdoac.goods_id,
                                                    vg.virtual_goods_id,
                                                    country_code,
                                                    c.cat_name,
                                                    platform
                    ) t1
               where project_name = 'floryday'
                 and impressions > 1000
                 and (sales_order > 2 and sales_order <100)
                 and users > 10
                 and cr > 10
                 and ctr > 1
                 and users > clicks
           ) t2
      where rn <= 4
     ) t3
where rn1 <= 8

union  all

select
       'sales',
       'airydress',
       goods_id,
       virtual_goods_id,
       country_code,
       cat_name,
       platform,
       impressions,
       sales_order,
       clicks,
       users,
       ctr,
       cr,
       rn,
       rn1
from (select goods_id,
             virtual_goods_id,
             country_code,
             cat_name,
             platform,
             impressions,
             sales_order,
             clicks,
             users,
             ctr,
             cr,
             rn,
             row_number() over (partition by cat_name,platform order by cr desc) rn1
      from (
               select goods_id,
                      virtual_goods_id,
                      country_code,
                      cat_name,
                      platform,
                      impressions,
                      sales_order,
                      clicks,
                      users,
                      ctr,
                      cr,
                      row_number() over (partition by country_code,cat_name,platform order by cr desc ) rn
               from (select 
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
                            (sum(clicks) / sum(impressions)) * (sum(sales_order) / sum(users)) * 10000 as cr
                     from tmp.tmp_fd_goods_display_order_artemis_country_full gdoac
                              left join tmp.tmp_fd_goods_full g on gdoac.goods_id = g.goods_id
                              left join tmp.tmp_fd_virtual_goods_full vg
                                        on gdoac.goods_id = vg.goods_id and gdoac.project_name = vg.project_name
                              left join tmp.tmp_fd_category_full c on g.cat_id = c.cat_id
                              GROUP by              gdoac.project_name,
                                                    gdoac.goods_id,
                                                    vg.virtual_goods_id,
                                                    country_code,
                                                    c.cat_name,
                                                    platform
                    ) t1
               where project_name = 'airydress'
                 and impressions > 500
                 and sales_order >= 20
                 and users > 5
                 and cr > 5
                 and ctr > 1
                 and users > clicks
           ) t2
      where rn <= 4
     ) t3
where rn1 <= 8


union all

select
       'potential',
       'airydress',
       goods_id,
       virtual_goods_id,
       country_code,
       cat_name,
       platform,
       impressions,
       sales_order,
       clicks,
       users,
       ctr,
       cr,
       rn,
       rn1
from (select goods_id,
             virtual_goods_id,
             country_code,
             cat_name,
             platform,
             impressions,
             sales_order,
             clicks,
             users,
             ctr,
             cr,
             rn,
             row_number() over (partition by cat_name,platform order by cr desc) rn1
      from (
               select goods_id,
                      virtual_goods_id,
                      country_code,
                      cat_name,
                      platform,
                      impressions,
                      sales_order,
                      clicks,
                      users,
                      ctr,
                      cr,
                      row_number() over (partition by country_code,cat_name,platform order by cr desc ) rn
               from (select 
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
                            (sum(clicks) / sum(impressions)) * (sum(sales_order) / sum(users)) * 10000 as cr
                     from tmp.tmp_fd_goods_display_order_artemis_country_full gdoac
                              left join tmp.tmp_fd_goods_full g on gdoac.goods_id = g.goods_id
                              left join tmp.tmp_fd_virtual_goods_full vg
                                        on gdoac.goods_id = vg.goods_id and gdoac.project_name = vg.project_name
                              left join tmp.tmp_fd_category_full c on g.cat_id = c.cat_id
                              GROUP by              gdoac.project_name,
                                                    gdoac.goods_id,
                                                    vg.virtual_goods_id,
                                                    country_code,
                                                    c.cat_name,
                                                    platform
                    ) t1
               where project_name = 'airydress'
                 and impressions > 500
                 and (sales_order > 1 and sales_order<20)
                 and users > 5
                 and cr > 5
                 and ctr > 1
                 and users > clicks
           ) t2
      where rn <= 4
     ) t3
where rn1 <= 8

union all

select
       'cart',
      project_name,
       goods_id,
       virtual_goods_id,
       country_code,
       cat_name,
       platform,
       impressions,
       sales_order,
       clicks,
       users,
       ctr,
       cr,
       rn,
       rn1
from (select
              project_name,
             goods_id,
             virtual_goods_id,
             country_code,
             cat_name,
             platform,
             impressions,
             sales_order,
             clicks,
             users,
             ctr,
             cr,
             rn,
             row_number() over (partition by cat_name,platform order by cr desc) rn1
      from (
               select
                      project_name,
                      goods_id,
                      virtual_goods_id,
                      country_code,
                      cat_name,
                      platform,
                      impressions,
                      sales_order,
                      clicks,
                      users,
                      ctr,
                      cr,
                      row_number() over (partition by country_code,cat_name,platform order by cr desc ) rn
               from (select
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
                            ( sum(clicks) / sum(impressions)) * 100                           as ctr,
                            (sum(clicks) / sum(impressions)) * (sum(sales_order) / sum(users)) * 10000 as cr,
                            (sum(detail_add_cart) / sum(users))*100  as add_rate
                     from tmp.tmp_fd_goods_display_order_artemis_country_full gdoac
                              left join tmp.tmp_fd_goods_full g on gdoac.goods_id = g.goods_id
                              left join tmp.tmp_fd_virtual_goods_full vg
                                        on gdoac.goods_id = vg.goods_id and gdoac.project_name = vg.project_name
                              left join tmp.tmp_fd_category_full c on g.cat_id = c.cat_id  where gdoac.project_name  in('floryday','airydress')
                    group by gdoac.goods_id,gdoac.project_name, country_code, platform,vg.virtual_goods_id,c.cat_name
                    ) t1
               where  impressions > 1000
                 and sales_order >= 2
                 and users > 100
                 and add_rate>25
                 and users > clicks
           ) t2
      where rn <= 2
     ) t3
where rn1 <= 4

union all

select
       'kr',
      project_name,
       goods_id,
       virtual_goods_id,
       country_code,
       cat_name,
       platform,
       impressions,
       sales_order,
       clicks,
       users,
       ctr,
       cr,
       rn,
       rn1
from (select
              project_name,
             goods_id,
             virtual_goods_id,
             country_code,
             cat_name,
             platform,
             impressions,
             sales_order,
             clicks,
             users,
             ctr,
             cr,
             rn,
             row_number() over (partition by cat_name,platform order by cr desc) rn1
      from (
               select
                      project_name,
                      goods_id,
                      virtual_goods_id,
                      country_code,
                      cat_name,
                      platform,
                      impressions,
                      sales_order,
                      clicks,
                      users,
                      ctr,
                      cr,
                      row_number() over (partition by country_code,cat_name,platform order by cr desc ) rn
               from (select
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
                            ( sum(clicks) / sum(impressions)) * 100                           as ctr,
                            (sum(clicks) / sum(impressions)) * (sum(sales_order) / sum(users)) * 10000 as cr,
                            (sum(checkout) / sum(users))*100  as kr
                     from tmp.tmp_fd_goods_display_order_artemis_country_full gdoac
                              left join tmp.tmp_fd_goods_full g on gdoac.goods_id = g.goods_id
                              left join tmp.tmp_fd_virtual_goods_full vg
                                        on gdoac.goods_id = vg.goods_id and gdoac.project_name = vg.project_name
                              left join tmp.tmp_fd_category_full c on g.cat_id = c.cat_id  
                              where gdoac.project_name  in('floryday','airydress')
                    group by gdoac.goods_id,gdoac.project_name, country_code, platform,vg.virtual_goods_id,c.cat_name
                    ) t1
               where  impressions > 1000
                 and sales_order >= 2
                 and users > 100
                 and kr>15
                 and users > clicks
           ) t2
      where rn <= 2
     ) t3
where rn1 <= 4

union all

select
       'rate',
      project_name,
       goods_id,
       virtual_goods_id,
       country_code,
       cat_name,
       platform,
       impressions,
       sales_order,
       clicks,
       users,
       ctr,
       cr,
       rn,
       rn1
from (select
              project_name,
             goods_id,
             virtual_goods_id,
             country_code,
             cat_name,
             platform,
             impressions,
             sales_order,
             clicks,
             users,
             ctr,
             cr,
             rn,
             row_number() over (partition by cat_name,platform order by cr desc) rn1
      from (
               select
                      project_name,
                      goods_id,
                      virtual_goods_id,
                      country_code,
                      cat_name,
                      platform,
                      impressions,
                      sales_order,
                      clicks,
                      users,
                      ctr,
                      cr,
                      row_number() over (partition by country_code,cat_name,platform order by cr desc ) rn
               from (select
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
                            ( sum(clicks) / sum(impressions)) * 100                           as ctr,
                            (sum(clicks) / sum(impressions)) * (sum(sales_order) / sum(users)) * 10000 as cr,
                            (sum(sales_order) / sum(users))*100  as rate
                     from tmp.tmp_fd_goods_display_order_artemis_country_full gdoac
                              left join tmp.tmp_fd_goods_full g on gdoac.goods_id = g.goods_id
                              left join tmp.tmp_fd_virtual_goods_full vg
                                        on gdoac.goods_id = vg.goods_id and gdoac.project_name = vg.project_name
                              left join tmp.tmp_fd_category_full c on g.cat_id = c.cat_id  
                              where gdoac.project_name  in('floryday','airydress')
                    group by gdoac.goods_id,gdoac.project_name, country_code, platform,vg.virtual_goods_id,c.cat_name
                    ) t1
               where  impressions > 1000
                  and clicks>0
                 and sales_order >= 2
                 and users > 9
                 and rate>5
                 and users > clicks
           ) t2
      where rn <= 2
     ) t3
where rn1 <= 4;