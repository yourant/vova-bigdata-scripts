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
FROM (
    SELECT
        nvl(tab.project_name,'all') as project_name,
        nvl(tab.cat_name,'all') as cat_name,
        nvl(tab.provider_attribute_name,'all') as provider_attribute_name,
        nvl(tab.goods_style,'all') as goods_style,

        count(distinct if(is_on_sale = 1, virtual_goods_id, null)) as on_sale_goods_num, --在架商品数
        count(distinct if(is_on_sale = 1 and goods_sales_number > 0, virtual_goods_id, null)) as dynamic_goods_num, --动销商品数
        count(distinct if(up_sale = 'yes', virtual_goods_id, null)) as up_sale_gooods,--上架商品数
        count(distinct if(down_sale = 'yes', virtual_goods_id, null)) as down_sale_gooods,--下架商品数
        count(distinct if(goods_sales_number >= 100, virtual_goods_id, null)) as goods_sale_100, --日销100件
        count(distinct if(goods_sales_number >= 50 and goods_sales_number < 100, virtual_goods_id, null)) as goods_sale_50, --日销50件
        count(distinct if(goods_sales_number >= 10 and goods_sales_number < 50, virtual_goods_id, null)) as goods_sale_10, --日销10件
        count(distinct if(goods_sales_number >= 50, virtual_goods_id, null)) as sale_good_goods, --畅销的商品
        count(distinct if(goods_sales_number >= 2 and goods_sales_number < 10, virtual_goods_id, null)) as goods_sale_2, --日销2件
        count(distinct if(goods_sales_number >= 0 and goods_sales_number < 2, virtual_goods_id, null)) as goods_sale_0 --日销0-2件

    FROM (

        SELECT
            /*+ REPARTITION(1) */
            rgp.project_name,
            rgp.goods_id,
            rgp.virtual_goods_id,
            rgp.cat_id,
            rgp.cat_name,
            rgp.is_on_sale,
            rgp.on_sale_time_utc,
            DATEDIFF('${pt}', to_date(rgp.on_sale_time_utc)) as diff_sale_day,
            gppa.department_type as provider_attribute_name,
            nvl(og.goods_sales_number,0) as goods_sales_number,
            if(rgp.is_on_sale = 1 and DATEDIFF('${pt}', to_date(rgp.on_sale_time_utc)) <= 30,'new_style','old_style') as goods_style,
            if(rgp.is_on_sale = 1 and '${pt}' = to_date(rgp.on_sale_time_utc),'yes','no') as up_sale,
            if(rgp.is_on_sale = 0 and '${pt}' = to_date(rgp.on_sale_time_utc),'yes','no') as down_sale
        FROM (

            --计算商品的基本信息
            select /*+ REPARTITION(1) */
               gp.project_name as project_name,
               gp.goods_id as goods_id,
               g.virtual_goods_id as virtual_goods_id,

               gp.is_on_sale_tag as is_on_sale_tag,
               gp.old_value,
               gp.new_value,
               gp.is_on_sale as is_on_sale,
               to_utc_timestamp(gp.on_sale_time, 'America/Los_Angeles') as on_sale_time_utc,

               g.cat_id as cat_id,
               g.cat_name as cat_name,
               '${pt}' as calculate_date,
               gp.rank
            from(

              select
                    project_name,
                    goods_id,
                    is_on_sale_tag,
                    old_value,
                    new_value,
                    is_on_sale,
                    on_sale_time,
                    rank
              from(
                    select
                        lower(project_name) as project_name,
                        field_id as goods_id,
                        field_name as is_on_sale_tag,
                        old_value,
                        new_value,
                        case
                            when new_value = 1 and field_name = 'is_on_sale' then 1 --上架
                            when new_value = 0 and field_name = 'is_on_sale' then 0 --下架
                            else 2
                        end as is_on_sale,--上下架判断
                        modify_time as on_sale_time,
                        row_number() over (partition by lower(project_name),field_id order by modify_time desc) as rank
                  from ods_fd_vb.ods_fd_project_goods_history
                  where lower(project_name) IN ('floryday','airydress')
                  and table_name = 'goods_project'
                  and field_name ='is_on_sale'
                  and field_id_name ='goods_id'
              )tab where tab.rank = 1

            )gp
            left join dim.dim_fd_goods g on (gp.goods_id = g.goods_id and gp.project_name = lower(g.project_name))
            where g.virtual_goods_id is not null

         ) rgp

        --计算商品的供应商
         LEFT JOIN ods_fd_ecshop.ods_fd_goods_provider_department gppa on gppa.goods_id = rgp.goods_id

        --计算商品销量
         LEFT JOIN (

              SELECT project_name,virtual_goods_id,pay_date,sum(goods_number) as goods_sales_number
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
                  WHERE date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}'
                  AND goods_id is not null
                  AND goods_id > 0
                  AND pay_status = 2
                  AND lower(project_name) IN ('floryday','airydress')
              )ogg GROUP BY project_name,virtual_goods_id,pay_date

        ) og ON (rgp.virtual_goods_id = og.virtual_goods_id and rgp.calculate_date = og.pay_date)
        where gppa.department_type in('贸综','工厂') --只保留贸综，工厂的数据

    )tab group by tab.project_name, tab.cat_name, tab.provider_attribute_name, tab.goods_style with cube
)tab2 where tab2.project_name  != 'all';