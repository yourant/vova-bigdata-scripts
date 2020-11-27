insert overwrite table dwd.dwd_fd_order_goods_top partition (pt = '${pt3}')
select  /*+ REPARTITION(1) */
 virtual_goods_id,goods_id,cat_name,purchase_price,goods_type
from (
    select   tab3.virtual_goods_id,
             tab3.goods_id,
             tab3.cat_name,
             tab3.purchase_price,
             tab3.goods_type,
             row_number() over (PARTITION BY  tab3.virtual_goods_id,tab3.goods_id,tab3.cat_name order by tab3.goods_type asc) as rn
    from (
        select nvl(tab1.virtual_goods_id,'UNKNOWN') as virtual_goods_id, nvl(tab1.goods_id,'UNKNOWN') as goods_id, nvl(tab1.cat_name,'UNKNOWN') as cat_name,nvl(tab2.purchase_price,0.0) as purchase_price,tab1.goods_type as goods_type
        from (
              select ogio.virtual_goods_id,ogio.goods_id,ogio.cat_name,'0' as goods_type
              from (
                  select cast(ogi.virtual_goods_id as string) as virtual_goods_id,
                         ogi.goods_id as goods_id,
                         ogi.cat_name as cat_name,
                         row_number() over (order by ogi.total_goods_number desc) rn
                  from (
                           select virtual_goods_id,
                                  goods_id,
                                  cat_name,
                                  sum(goods_number) as total_goods_number
                           from dwd.dwd_fd_order_goods
                           where date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) < '${pt3}'
                             and date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) >= '${pt11}'
                             and lower(project_name) = 'floryday'
                             and pay_status = 2
                             and virtual_goods_id is not null
                             and goods_id is not null
                             and cat_name is not null
                           group by virtual_goods_id, goods_id,cat_name
                       ) ogi
              ) ogio where ogio.rn <= 300

              union all
              select ogio2.virtual_goods_id,ogio2.goods_id,ogio2.cat_name,'0' as goods_type
                    from (
                        select cast(ogi2.virtual_goods_id as string) as virtual_goods_id,
                               ogi2.goods_id as goods_id,
                               ogi2.cat_name as cat_name,
                               row_number() over (order by ogi2.total_goods_number desc) rn
                        from (
                                 select virtual_goods_id,
                                        goods_id,
                                        cat_name,
                                        sum(goods_number) as total_goods_number
                                 from dwd.dwd_fd_order_goods
                                 where date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) < '${pt3}'
                                   and date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) >= '${pt11}'
                                   and lower(project_name) = 'airydress'
                                   and pay_status = 2
                                   and virtual_goods_id is not null
                                   and goods_id is not null
                                   and cat_name is not null
                                 group by virtual_goods_id, goods_id,cat_name
                             ) ogi2
              ) ogio2 where ogio2.rn <= 100

              union all
              select
                    cast(tab1.virtual_goods_id as string) as virtual_goods_id,
                    tab2.goods_id,
                    tab1.cat_name,
                    '1' as goods_type
                from (
                    select
                        lower(project_name) as project_name,
                        virtual_goods_id,
                        cat_name
                    from dwd.dwd_fd_finished_goods_test
                    where to_date(finish_time) = '${pt}'
                    and lower(project_name) in ('floryday','airydress')
                    and result = 1
                    group by lower(project_name),virtual_goods_id,cat_name
                )tab1
                left join(
                    select
                        lower(project_name) as project_name,
                        virtual_goods_id,
                        goods_id,
                        cat_name,
                        sum(goods_number) as total_goods_number
                     from dwd.dwd_fd_order_goods
                     where date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) <= '${pt}'
                       and date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) >= '${pt11}'
                       and lower(project_name) in ('floryday','airydress')
                       and pay_status = 2
                       and virtual_goods_id is not null
                       and goods_id is not null
                       and cat_name is not null
                     group by lower(project_name),virtual_goods_id, goods_id,cat_name
                ) tab2 on tab1.project_name = tab2.project_name and tab1.virtual_goods_id =  tab2.virtual_goods_id
                where tab2.goods_id is not null

        )tab1
        left join (
           select goods_id,purchase_price
             from ods_fd_vb.ods_fd_goods_purchase_price
            group by goods_id,purchase_price
        ) tab2 on tab2.goods_id = tab1.goods_id
    ) tab3
) tab4 where tab4.rn = 1;