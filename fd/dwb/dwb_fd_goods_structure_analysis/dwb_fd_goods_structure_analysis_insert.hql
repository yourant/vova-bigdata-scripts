insert overwrite table dwb.dwb_fd_goods_structure_analysis partition(pt='${pt}')
SELECT
     /*+ REPARTITION(1) */
     project_name,
     cat_name,
     provider_attribute_name,
     goods_style,

     on_sale_goods_num,
     dynamic_goods_num,
     up_sale_gooods,
     down_sale_gooods,
     goods_sale_100,
     goods_sale_50,
     goods_sale_10,
     sale_good_goods,
     goods_sale_2,
     goods_sale_0
FROM
    (SELECT
        nvl(tab.project_name,'all') as project_name,
        nvl(tab.cat_name,'all') as cat_name,
        nvl(tab.provider_attribute_name,'all') as provider_attribute_name,
        nvl(tab.goods_style,'all') as goods_style,

        count(distinct if(on_sale = 1 and is_delete = 0 and is_display = 1, virtual_goods_id, null)) as on_sale_goods_num,--在架商品数
        count(distinct if(on_sale = 1 and is_delete = 0 and is_display = 1 and goods_sales_number > 0, virtual_goods_id, null)) as dynamic_goods_num, --动销商品数
        count(distinct if(up_sale = 'yes', virtual_goods_id, null)) as up_sale_gooods,--上架商品数
        count(distinct if(down_sale = 'yes', virtual_goods_id, null)) as down_sale_gooods,--下架商品数
        count(distinct if(on_sale = 1 and is_delete = 0 and is_display = 1 and goods_sales_number >= 100, virtual_goods_id, null)) as goods_sale_100, --日销100件
        count(distinct if(on_sale = 1 and is_delete = 0 and is_display = 1 and goods_sales_number >= 50 and goods_sales_number < 100, virtual_goods_id, null)) as goods_sale_50, --日销50件
        count(distinct if(on_sale = 1 and is_delete = 0 and is_display = 1 and goods_sales_number >= 10 and goods_sales_number < 50, virtual_goods_id, null)) as goods_sale_10, --日销10件
        count(distinct if(on_sale = 1 and is_delete = 0 and is_display = 1 and goods_sales_number >= 50, virtual_goods_id, null)) as sale_good_goods, --畅销的商品
        count(distinct if(on_sale = 1 and is_delete = 0 and is_display = 1 and goods_sales_number >= 2 and goods_sales_number < 10, virtual_goods_id, null)) as goods_sale_2, --日销2件
        count(distinct if(on_sale = 1 and is_delete = 0 and is_display = 1 and goods_sales_number >= 0 and goods_sales_number < 2, virtual_goods_id, null)) as goods_sale_0 --日销0-2件
    FROM
        (SELECT
            lower(project_name) as project_name,
            goods_id,
            virtual_goods_id,
            cat_id,
            cat_name,
            diff_sale_day,
            provider_attribute_name,
            goods_sales_number,
            goods_style,
            up_sale,
            down_sale,

            --获取is_on_sale,is_delete,is_display
            on_sale,
            is_delete,
            is_display

        FROM
            (SELECT
              rgp.project_name,
              rgp.goods_id,
              rgp.virtual_goods_id,
              rgp.cat_id,
              rgp.cat_name,
              rgp.is_on_sale,
              rgp.on_sale_time_utc,
              DATEDIFF('2021-05-27', to_date(rgp.on_sale_time_utc)) as diff_sale_day,
              NVL(gppa.department_type,'测试款') as provider_attribute_name,
              nvl(og.goods_sales_number,0) as goods_sales_number,
              if(rgp.is_on_sale = 1 and rgpp.on_sale_cnt = 1 and DATEDIFF('2021-05-27', to_date(rgp.on_sale_time_utc)) <= 30,'新款','老款') as goods_style,
              if(rgp.is_on_sale = 1 and '2021-05-27' = to_date(rgp.on_sale_time_utc),'yes','no') as up_sale,
              if(rgp.is_on_sale = 0 and '2021-05-27' = to_date(rgp.on_sale_time_utc),'yes','no') as down_sale,
              --获取is_on_sale,is_delete,is_display
              rgp.on_sale,
              rgp.is_delete,
              rgp.is_display
            FROM(
                --计算商品的基本信息
                select
                   fgp.project_name as project_name,
                   fgp.goods_id as goods_id,
                   g.virtual_goods_id as virtual_goods_id,

                   gp.old_value,
                   gp.new_value,
                   gp.is_on_sale as is_on_sale,
                   to_utc_timestamp(gp.on_sale_time, 'America/Los_Angeles') as on_sale_time_utc,

                   g.cat_id as cat_id,
                   g.cat_name as cat_name,
                   '2021-05-27' as calculate_date,

                   --获取is_on_sale,is_delete,is_display
                   fgp.is_on_sale as on_sale,
                   fgp.is_delete,
                   fgp.is_display
                from
                (
                    select
                        project_name,
                        goods_id,
                        is_on_sale,
                        is_delete,
                        is_display
                    from
                        ods_fd_vb.ods_fd_goods_project
                    where
                        lower(project_name) in ('floryday','airydress')
                )fgp

                left join
                    (
                    select
                        project_name,
                        goods_id,
                        is_on_sale_tag,
                        old_value,
                        new_value,
                        is_on_sale,
                        on_sale_time
                    from(
                        select
                            lower(project_name) as project_name,
                            field_id as goods_id,
                            field_name as is_on_sale_tag,
                            old_value,
                            new_value,
                            case
                                when new_value = '1' and field_name = 'is_on_sale' then 1 --上架
                                when new_value = '0' and field_name = 'is_on_sale' then 0 --下架
                                else 2
                            end as is_on_sale,--上下架判断
                            modify_time as on_sale_time,
                            row_number() over (partition by lower(project_name),field_id order by modify_time desc) as rn
                        from
                            ods_fd_vb.ods_fd_project_goods_history
                        where
                            lower(project_name) IN ('floryday','airydress')
                        )tab
                    where
                        tab.rn = 1
                    )gp
                on
                    fgp.goods_id = gp.goods_id
                    and lower(fgp.project_name) = lower(gp.project_name)

                left join
                    dim.dim_fd_goods g
                on
                    gp.goods_id = g.goods_id
                    and lower(gp.project_name) = lower(g.project_name)

                where
                    g.virtual_goods_id is not null
                )rgp

            --计算商品最近30天上架次数
            LEFT JOIN(
                select
                    lower(project_name) as project_name,
                    field_id as goods_id,
                    count(if(new_value = '1' and field_name = 'is_on_sale',1,null)) as on_sale_cnt --上架次数

                from ods_fd_vb.ods_fd_project_goods_history

                where lower(project_name) IN ('floryday','airydress')
                and table_name = 'goods_project'
                and field_name ='is_on_sale'
                and field_id_name ='goods_id'
                and to_date(to_utc_timestamp(modify_time, 'America/Los_Angeles')) >= date_sub('2021-05-27',29)
                group by lower(project_name),field_id
            )rgpp
            on  rgpp.project_name = lower(rgp.project_name) and rgpp.goods_id = rgp.goods_id

            --计算商品的供应商
            LEFT JOIN ods_fd_ecshop.ods_fd_goods_provider_department gppa on gppa.goods_id = rgp.goods_id

            --计算商品销量
            LEFT JOIN
                (SELECT project_name,virtual_goods_id,pay_date,sum(goods_number) as goods_sales_number
                 FROM(
                      SELECT
                          lower(project_name) as project_name,
                          order_id,
                          pay_time,
                          cat_name,
                          country_code,
                          platform_type,
                          goods_id,
                          virtual_goods_id,
                          goods_number,
                          date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) as pay_date
                      FROM dwd.dwd_fd_order_goods
                      WHERE date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '2021-05-27'
                      AND goods_id is not null
                      AND goods_id > 0
                      AND pay_status = 2
                      AND lower(project_name) IN ('floryday','airydress')
                  )ogg GROUP BY project_name,virtual_goods_id,pay_date
                )og
                    ON (rgp.virtual_goods_id = og.virtual_goods_id and rgp.calculate_date = og.pay_date)
            )t
        where t.provider_attribute_name not in ('其他')
        )tab
    group by
        tab.project_name,
        tab.cat_name,
        tab.provider_attribute_name,
        tab.goods_style with cube
    )tab2
where
    tab2.project_name  != 'all'