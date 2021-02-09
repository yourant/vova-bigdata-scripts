#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.sql.crossJoin.enabled=true"  --conf "spark.dynamicAllocation.maxExecutors=100" --conf "spark.app.name=dwb_vova_search_rate_monitor" -e "
insert overwrite table dwb.dwb_vova_search_view_monitor_1 PARTITION (pt = '${cur_date}')
select '${cur_date}' cur_date,
       a.home_page_top_clk_cnt,
       a.sort_enter_top_clk_cnt,
       b.search_begin_uv,
       c.hot_words_expre_cnt,
       d.hot_words_clk_cnt,
       c.lenovo_words_expre_cnt,
       d.lenovo_words_clk_cnt
from (
         select count(distinct
                      if(element_name = 'searchtab' and page_code = 'homepage', device_id, null)) home_page_top_clk_cnt,
                count(distinct
                      if(element_name = 'searchtab' and page_code = 'category', device_id,
                         null))                                                                   sort_enter_top_clk_cnt
         from dwd.dwd_vova_log_common_click a
         where a.pt = '${cur_date}' and element_name = 'searchtab' and page_code in ('homepage','category')
     ) a
         left join
     (
         select count(distinct device_id) search_begin_uv
         from dwd.dwd_vova_log_page_view a
         where a.pt = '${cur_date}'
           and page_code = 'search_begin'
     ) b on 1 = 1
         left join (
    select count(distinct
                 if(list_type = '/hot_search' and element_name = 'hot_search' and page_code = 'search_begin', device_id,
                    null))                                                 hot_words_expre_cnt,
           count(distinct if(element_name = 'associationLists' and
                             page_code = 'search_begin', device_id, null)) lenovo_words_expre_cnt
    from dwd.dwd_vova_log_impressions a
    where a.pt = '${cur_date}'
) c on 1 = 1
left join (select
                count(distinct
                      if(list_type = '/hot_search' and element_name = 'hot_search' and page_code = 'search_begin',
                         device_id,
                         null))                                                                   hot_words_clk_cnt,
                count(distinct if(list_type = '/searchAssociationLists' and element_name = 'associationList' and
                                  page_code = 'search_begin', device_id, null))                   lenovo_words_clk_cnt
         from dwd.dwd_vova_log_click_arc a
         where a.pt = '${cur_date}' and a.datasource = 'vova') d on 1=1
;


insert overwrite table dwb.dwb_vova_search_view_monitor_2 PARTITION (pt = '${cur_date}')
select '${cur_date}'                                                     cur_date,
       a.goods_list,
       b.search_pv,
       b.search_uv,
       b.driv_search_pv,
       b.driv_search_uv,
       a.goods_expre_pv,
       a.goods_expre_uv,
       c.cart_uv,
       c.cart_pv,
       d.pay_uv,
       d.gmv,
       concat(nvl(round(c.cart_uv / a.goods_expre_uv * 100, 2), 0), '%') cart_rate,
       concat(nvl(round(d.pay_uv / a.goods_expre_uv * 100, 2), 0), '%')  pay_rate,
       nvl(round(d.gmv / a.goods_expre_uv, 4), 0)     gmv_cr
from (
         select nvl(case
                    when list_type in ('/search_result_recommend', '/search_result', '/search_result_sold',
                                           '/search_result_price_desc', '/search_result_price_asc',
                                           '/search_result_newarrival') then '搜索结果页'
                    when list_type in ('/search_result_also_like') then '搜索无结果页'
                    else 'NA' end,'all')         goods_list,
                count(1)                  goods_expre_pv,
                count(distinct device_id) goods_expre_uv
         from dwd.dwd_vova_log_goods_impression a
         where a.pt = '${cur_date}'
           and a.page_code = 'search_result'
         group by cube (case
                            when list_type in ('/search_result_recommend', '/search_result', '/search_result_sold',
                                                   '/search_result_price_desc', '/search_result_price_asc',
                                                   '/search_result_newarrival') then '搜索结果页'
                            when list_type in ('/search_result_also_like') then '搜索无结果页'
                            else 'NA' end)
     ) a
         left join (
    select
           count(1)                                                     search_pv,
           count(distinct device_id)                                    search_uv,
           sum(if(element_type = 'custom', 1, 0))                       driv_search_pv,
           count(distinct if(element_type = 'custom', device_id, null)) driv_search_uv
    from dwd.dwd_vova_log_common_click a
    where a.pt = '${cur_date}'
      and a.page_code = 'search_begin'
) b on 1=1
         left join (
    select nvl(case
               when pre_list_type in
                    ('/search_result_recommend', '/search_result', '/search_result_sold', '/search_result_price_desc',
                     '/search_result_price_asc', '/search_result_newarrival') then '搜索结果页'
               when pre_list_type in ('/search_result_also_like') then '搜索无结果页'
               else 'NA' end,'all')         goods_list,
           count(1)                  cart_pv,
           count(distinct device_id) cart_uv
    from dwd.dwd_vova_fact_cart_cause_v2 a
    where a.pt = '${cur_date}'
      and a.pre_page_code = 'search_result'
    group by cube (case
                       when pre_list_type in ('/search_result_recommend', '/search_result', '/search_result_sold',
                                              '/search_result_price_desc', '/search_result_price_asc',
                                              '/search_result_newarrival') then '搜索结果页'
                       when pre_list_type in ('/search_result_also_like') then '搜索无结果页'
                       else 'NA' end)
) c on a.goods_list = c.goods_list
         left join (
    select nvl(case
               when pre_list_type in
                    ('/search_result_recommend', '/search_result', '/search_result_sold', '/search_result_price_desc',
                     '/search_result_price_asc', '/search_result_newarrival') then '搜索结果页'
               when pre_list_type in ('/search_result_also_like') then '搜索无结果页'
               else 'NA' end,'all')                                   goods_list,
           count(distinct a.device_id)                           pay_uv,
           sum(c.shop_price * c.goods_number + c.shipping_fee) gmv
    from dwd.dwd_vova_fact_order_cause_v2 a
             join dwd.dwd_vova_fact_pay c
                  on a.order_goods_id = c.order_goods_id
    where a.pt = '${cur_date}'
      and to_date(c.pay_time) = '${cur_date}'
      and a.pre_page_code = 'search_result'
    group by cube (case
                       when pre_list_type in ('/search_result_recommend', '/search_result', '/search_result_sold',
                                              '/search_result_price_desc', '/search_result_price_asc',
                                              '/search_result_newarrival') then '搜索结果页'
                       when pre_list_type in ('/search_result_also_like') then '搜索无结果页'
                       else 'NA' end)
) d on a.goods_list = d.goods_list
;



insert overwrite table dwb.dwb_vova_search_view_monitor_3 PARTITION (pt = '${cur_date}')
select '${cur_date}'                           cur_date,
       nvl(a.filter_name,b.filter_name),
       nvl(a.expre_pv,0),
       nvl(a.expre_uv,0),
       nvl(b.clk_pv,0),
       nvl(b.clk_uv,0),
       if(a.expre_pv is null,0,round(nvl(b.clk_pv / a.expre_pv, 0), 4)) ctr
from (
         select nvl(filter_name,'all') filter_name,
                count(1)                  expre_pv,
                count(distinct device_id) expre_uv
         from (
                  select case
                             when page_code = 'search_result' and element_name = 'toolBarProductsListSortA' then '左侧排序栏'
                             when page_code = 'search_result' and element_name = 'toolBarProductsListSortLayerList' and
                                  element_id like '%recommend_%' then '推荐排序'
                             when page_code = 'search_result' and element_name = 'toolBarProductsListSortLayerList' and
                                  element_id like '%new-arrival_%' then '新品排序'
                             when page_code = 'search_result' and element_name = 'toolBarProductsListSortLayerList' and
                                  element_id like '%price_asc%' then '价格由低到高'
                             when page_code = 'search_result' and element_name = 'toolBarProductsListSortLayerList' and
                                  element_id like '%price_desc%' then '价格由高到低'
                             when page_code = 'search_result' and element_name = 'toolBarProductsListFilterE' and
                                  element_id like '%special_flag_list__vova_express__selected%' then '运费'
                             when page_code = 'search_result' and element_name = 'toolBarProductsListSortB' then '销量'
                             when page_code = 'search_result' and element_name = 'toolBarProductsListStyleC'
                                 then '切换展示方式'
                             when page_code = 'search_result' and element_name = 'toolBarProductsListFilterD'
                                 then '右侧筛选浮层'
                             when page_code = 'search_result' and element_name = 'toolBarProductsListFilterLayerList'
                                 then '筛选-标签'
                             when page_code = 'search_result' and
                                  element_name = 'toolBarProductsListFilterLayerListPrice'
                                 then '筛选-价格'
                             else 'NA'
                             end filter_name,
                         device_id
                  from dwd.dwd_vova_log_impressions a
                  where a.pt = '${cur_date}' and page_code = 'search_result'
              ) t
         group by cube (filter_name)
     ) a
         full join (
    select nvl(filter_name,'all') filter_name,
           count(1)                  clk_pv,
           count(distinct device_id) clk_uv
    from (
             select case
                        when page_code = 'search_result' and element_name = 'toolBarProductsListSortA' then '左侧排序栏'
                        when page_code = 'search_result' and element_name = 'toolBarProductsListSortLayerList' and
                             element_id like '%recommend_%' then '推荐排序'
                        when page_code = 'search_result' and element_name = 'toolBarProductsListSortLayerList' and
                             element_id like '%new-arrival_%' then '新品排序'
                        when page_code = 'search_result' and element_name = 'toolBarProductsListSortLayerList' and
                             element_id like '%price_asc%' then '价格由低到高'
                        when page_code = 'search_result' and element_name = 'toolBarProductsListSortLayerList' and
                             element_id like '%price_desc%' then '价格由高到低'
                        when page_code = 'search_result' and element_name = 'toolBarProductsListFilterE' and
                             element_id like '%special_flag_list__vova_express__selected%' then '运费'
                        when page_code = 'search_result' and element_name = 'toolBarProductsListSortB' then '销量'
                        when page_code = 'search_result' and element_name = 'toolBarProductsListStyleC'
                            then '切换展示方式'
                        when page_code = 'search_result' and element_name = 'toolBarProductsListFilterD'
                            then '右侧筛选浮层'
                        when page_code = 'search_result' and element_name = 'toolBarProductsListFilterLayerList'
                            then '筛选-标签'
                        when page_code = 'search_result' and
                             element_name = 'toolBarProductsListFilterLayerListPrice'
                            then '筛选-价格'
                        else 'NA'
                        end filter_name,
                    device_id
             from dwd.dwd_vova_log_common_click a
             where a.pt = '${cur_date}' and page_code = 'search_result'
         ) t
    group by cube (filter_name)
) b on a.filter_name=b.filter_name
;
"

if [ $? -ne 0 ];then
  exit 1
fi
