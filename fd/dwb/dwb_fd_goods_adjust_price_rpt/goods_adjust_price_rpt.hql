insert overwrite table dwb.dwb_fd_goods_adjust_price_rpt partition (pt = '${pt}')
select /*+ REPARTITION(1) */'${pt3}' as adjust_date,
     nvl(tab4.goods_id,'all') as goods_id,
     tab1.virtual_goods_id as virtual_goods_id,
     tab1.country_code as country_code,
     tab1.project_name as project,
     tab1.platform as platform_type,
     nvl(tab4.cat_name,'all') as cat_name,
     nvl(tab4.purchase_price,'0') as purchase_price,
     nvl(cast(tab1.avg_shop_price_before as decimal(10,6)),0.000000) as avg_shop_price_before,
     nvl(cast(tab1.avg_shop_price_after as decimal(10,6)),0.000000) as avg_shop_price_after,
     nvl(cast(tab1.adjust_price_range as decimal(10,6)),0.000000) as adjust_price_range,
     nvl(tab1.goods_number_before,0) as goods_number_before,
     nvl(tab1.goods_number_after,0) as goods_number_after,
     nvl(cast(tab1.shop_price_before as decimal(10,2)),0.00) as shop_price_before,
     nvl(cast(tab1.shop_price_after as decimal(10,2)),0.00) as shop_price_after,
     nvl(cast(tab2.total_shop_price_before as decimal(16,6)),0.000000) as total_shop_price_before,
     nvl(cast(tab2.total_shop_price_after as decimal(16,6)),0.000000) as total_shop_price_after,
     nvl(cast((tab1.shop_price_before / tab2.total_shop_price_before) as decimal(10,6)),0.000000) as shop_amount_rate_before,
     nvl(cast((tab1.shop_price_after /tab2.total_shop_price_after) as decimal(10,6)),0.000000) as shop_amount_rate_after,
     nvl(cast(tab3.add_rate_before as decimal(10,6)),0.000000) as add_rate_before,
     nvl(cast(tab3.add_rate_after as decimal(10,6)),0.000000) as add_rate_after,
     nvl(cast(tab3.ctr_before as decimal(10,6)),0.000000) as ctr_before,
     nvl(cast(tab3.ctr_after as decimal(10,6)),0.000000) as ctr_after,
     nvl(cast(tab3.rate_before as decimal(10,6)),0.000000) as rate_before,
     nvl(cast(tab3.rate_after as decimal(10,6)),0.000000) as rate_after,
     nvl(cast(tab3.cr_before as decimal(10,6)),0.000000) as cr_before,
     nvl(cast(tab3.cr_after as decimal(10,6)),0.000000) as cr_after,
     nvl(tab3.click_before,0) as click_before,
     nvl(tab3.click_after,0) as click_after,
     nvl(tab3.impression_before,0) as imp_before,
     nvl(tab3.impression_after,0) as imp_after,
     case when tab4.goods_type = '1' then '测款成功商品' else '非测款成功商品' end as goods_type
from (
    select
        nvl(ogi1.virtual_goods_id,'all') as virtual_goods_id,
        nvl(ogi2.country_code,'all') as country_code,
        nvl(ogi2.project_name,'all') as project_name,
        nvl(ogi2.platform,'all') as platform,
        cast(sum(if(ogi2.pt >= date_sub('${pt3}',3) and ogi2.pt <= date_sub('${pt3}',1),ogi2.shop_price,0.0)) as float) as shop_price_before,
        cast(sum(if(ogi2.pt >= date_add('${pt3}',1) and ogi2.pt <= date_add('${pt3}',3),ogi2.shop_price,0.0)) as float) as shop_price_after,
        cast(sum(if(ogi2.pt >= date_sub('${pt3}',3) and ogi2.pt <= date_sub('${pt3}',1),ogi2.goods_number,0)) as float) as goods_number_before,
        cast(sum(if(ogi2.pt >= date_add('${pt3}',1) and ogi2.pt <= date_add('${pt3}',3),ogi2.goods_number,0)) as float) as goods_number_after,
        (cast(sum(if(ogi2.pt >= date_sub('${pt3}',3) and ogi2.pt <= date_sub('${pt3}',1),ogi2.shop_price,0)) as float) / cast(sum(if(ogi2.pt >= date_sub('${pt3}',3) and ogi2.pt <= date_sub('${pt3}',1),ogi2.goods_number,0)) as float)) as avg_shop_price_before,
        (cast(sum(if(ogi2.pt >= date_add('${pt3}',1) and ogi2.pt <= date_add('${pt3}',3),ogi2.shop_price,0)) as float) / cast(sum(if(ogi2.pt >= date_add('${pt3}',1) and ogi2.pt <= date_add('${pt3}',3),ogi2.goods_number,0)) as float)) as avg_shop_price_after,
        (((cast(sum(if(ogi2.pt >= date_add('${pt3}',1) and ogi2.pt <= date_add('${pt3}',3),ogi2.shop_price,0)) as float) / cast(sum(if(ogi2.pt >= date_add('${pt3}',1) and ogi2.pt <= date_add('${pt3}',3),ogi2.goods_number,0)) as float))/ (cast(sum(if(ogi2.pt >= date_sub('${pt3}',3) and ogi2.pt <= date_sub('${pt3}',1),ogi2.shop_price,0)) as float) / cast(sum(if(ogi2.pt >= date_sub('${pt3}',3) and ogi2.pt <= date_sub('${pt3}',1),ogi2.goods_number,0)) as float))) -1) as adjust_price_range
    from (select virtual_goods_id,goods_id,cat_name from dwd.dwd_fd_order_goods_top  where pt = '${pt3}') ogi1
    left join (
		select
            lower(t.project_name) as project_name,
            t.country_code        as country_code,
            case
                when t.is_app = 0 and t.device_type in ('pc', 'pad') then 'web'
                when t.is_app = 0 and t.device_type = 'mobile' then 'h5'
                when t.is_app = 1 then 'mob'
                else 'others' end   as platform,
			date(from_unixtime(t.pay_time,'yyyy-MM-dd hh:mm:ss')) as pt,
			t.goods_number,
			t.shop_price,
			t.virtual_goods_id
			from dwd.dwd_fd_order_goods t
            where date(from_unixtime(t.pay_time,'yyyy-MM-dd hh:mm:ss')) >= date_sub('${pt3}', 3)
            and date(from_unixtime(t.pay_time,'yyyy-MM-dd hh:mm:ss')) <= date_add('${pt3}', 3)
            and t.virtual_goods_id is not null  and t.pay_status = 2
			and upper(t.country_code) in ('DE', 'FR', 'GB', 'US', 'SE', 'IT', 'ES', 'NL', 'NO', 'MX', 'CH', 'DK', 'PL', 'SA', 'BE', 'AT', 'AU', 'RU', 'FI', 'CA') /* 限制国家 */
    ) ogi2 on ogi2.virtual_goods_id= ogi1.virtual_goods_id
    group by ogi1.virtual_goods_id,ogi2.country_code,ogi2.project_name,ogi2.platform with cube
) tab1
left join (
    select
       nvl(ogi2.country_code,'all') as country_code,
       nvl(ogi2.project_name,'all') as project_name,
       nvl(ogi2.platform,'all') as platform,
       cast(sum(if(ogi2.pt >= date_sub('${pt3}',3) and ogi2.pt <= date_sub('${pt3}',1),ogi2.shop_price,0)) as float) as total_shop_price_before,
       cast(sum(if(ogi2.pt >= date_add('${pt3}',1) and ogi2.pt <= date_add('${pt3}',3),ogi2.shop_price,0)) as float) as total_shop_price_after
    from (select virtual_goods_id,goods_id,cat_name from dwd.dwd_fd_order_goods_top where pt = '${pt3}') ogi1
    left join (
		select lower(t.project_name) as project_name,
            t.country_code        as country_code,
			case
                when t.is_app = 0 and t.device_type in ('pc', 'pad') then 'web'
                when t.is_app = 0 and t.device_type = 'mobile' then 'h5'
                when t.is_app = 1 then 'mob'
            else 'others' end   as platform,
			date(from_unixtime(t.pay_time,'yyyy-MM-dd hh:mm:ss')) as pt,
			t.goods_number,
			t.shop_price,
			t.virtual_goods_id
			from dwd.dwd_fd_order_goods t
            where date(from_unixtime(t.pay_time,'yyyy-MM-dd hh:mm:ss')) >= date_sub('${pt3}', 3)
            and date(from_unixtime(t.pay_time,'yyyy-MM-dd hh:mm:ss')) <= date_add('${pt3}', 3)
            and t.virtual_goods_id is not null  and t.pay_status = 2

    ) ogi2 on ogi2.virtual_goods_id= ogi1.virtual_goods_id
    group by ogi2.country_code,ogi2.project_name,ogi2.platform with cube

)tab2 on (tab1.country_code = tab2.country_code and tab1.project_name = tab2.project_name and tab1.platform = tab2.platform)
left join (
    select
        virtual_goods_id,
        project,
        country_code,
        platform_type,
        add_rate_before,
        add_rate_after,
        rate_before,
        rate_after,
        ctr_before,
        ctr_after,
        cr_before,
        cr_after,
        click_before,
        click_after,
        impression_before,
        impression_after
    from dwb.dwb_fd_mid_goods_click_collect where pt = '${pt3}'
) tab3 on (tab1.virtual_goods_id = tab3.virtual_goods_id and tab1.project_name = tab3.project and tab1.country_code = tab3.country_code  and tab1.platform = tab3.platform_type)
left join(
  select virtual_goods_id,goods_id,cat_name,purchase_price,goods_type
  from dwd.dwd_fd_order_goods_top
  where pt = '${pt3}'
) tab4 on trim(tab1.virtual_goods_id) = trim(tab4.virtual_goods_id);