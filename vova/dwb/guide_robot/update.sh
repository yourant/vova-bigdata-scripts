#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期当天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi


spark-sql   --conf "spark.app.name=dwb_vova_shopping_guide_robot" --conf "spark.sql.autoBroadcastJoinThreshold=-1"  --conf "spark.sql.crossJoin.enabled=true"  --conf "spark.dynamicAllocation.maxExecutors=120"  -e "

insert overwrite table dwb.dwb_vova_shopping_guide_robot PARTITION (pt = '${cur_date}')
select
'${cur_date}' cur_date,a.list,a.expre_pv,a.expre_uv,b.clk_pv,b.clk_uv,nvl(c.cart_uv,0),nvl(d.pay_uv,0),nvl(d.gmv,0),e.enter_expre_pv, enter_expre_uv, enter_clk_pv, enter_clk_uv, button_expre_uv, button_clk_uv,f.session_expre_uv
from (
         select nvl(if(page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list', '会话界面',
                       'viewall列表'), 'all') list,
                count(1)                    expre_pv,
                count(distinct device_id)   expre_uv
         from dwd.dwd_vova_log_goods_impression a
         where pt = '${cur_date}' and dp = 'vova'
           and ((page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list') or
                (page_code = 'recommend_product_list' and list_type = '/robert_guide_also_like'))
         group by cube (if(page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list', '会话界面',
                           'viewall列表'))
     ) a
left join (
        select nvl(if(page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list', '会话界面', 'viewall列表'),
                   'all')                list,
               count(1)                  clk_pv,
               count(distinct device_id) clk_uv
        from dwd.dwd_vova_log_goods_click a
        where pt = '${cur_date}'and dp = 'vova'
          and ((page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list') or
               (page_code = 'recommend_product_list' and list_type = '/robert_guide_also_like'))
        group by cube (if(page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list', '会话界面',
                          'viewall列表'))
    ) b on a.list = b.list
left join (
    select nvl(if(pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list', '会话界面',
                  'viewall列表'), 'all') list,
           count(distinct device_id)   cart_uv
    from dwd.dwd_vova_fact_cart_cause_v2 a
    where pt = '${cur_date}' and a.datasource = 'vova'
      and ((pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list') or
           (pre_page_code = 'recommend_product_list' and pre_list_type = '/robert_guide_also_like'))
    group by cube (if(pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list', '会话界面',
                      'viewall列表'))
    ) c on a.list = c.list
left join (
    select nvl(if(pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list', '会话界面',
                  'viewall列表'), 'all') list,
           count(distinct a.device_id)   pay_uv,
           sum(b.shop_price * b.goods_number + b.shipping_fee) gmv
    from dwd.dwd_vova_fact_order_cause_v2 a
             join dwd.dwd_vova_fact_pay b on a.order_goods_id = b.order_goods_id
    where a.pt = '${cur_date}' and a.datasource = 'vova' and to_date(b.pay_time) = '${cur_date}'
      and ((pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list') or
           (pre_page_code = 'recommend_product_list' and pre_list_type = '/robert_guide_also_like'))
    group by cube (if(pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list', '会话界面',
                      'viewall列表'))
    ) d on a.list = d.list
left join (
    select
    'all' list,
    sum(enter_expre_pv) enter_expre_pv,
    sum(enter_expre_uv) enter_expre_uv,
    sum(enter_clk_pv) enter_clk_pv,
    sum(enter_clk_uv) enter_clk_uv,
    sum(button_expre_uv) button_expre_uv,
    sum(button_clk_uv) button_clk_uv
from (
         select sum(if(page_code = 'homepage'  and element_name = 'shopping_guide_robot',1, 0))                       enter_expre_pv,
                count(distinct if(page_code = 'homepage' and element_name = 'shopping_guide_robot', device_id, null)) enter_expre_uv,
                0                                                                          enter_clk_pv,
                0                                                                          enter_clk_uv,
                count(distinct if(page_code = 'robert_guide_session' and element_name = 'robert_viewall_impression', device_id,null))  button_expre_uv,
                0                                                                          button_clk_uv
         from dwd.dwd_vova_log_impressions a
         where pt = '${cur_date}'  and a.dp = 'vova'
         union all
         select 0                                                                          enter_expre_pv,
                0                                                                          enter_expre_uv,
                sum(if(page_code = 'homepage' and element_name = 'shopping_guide_robot', 1, 0))                       enter_clk_pv,
                count(distinct if(page_code = 'homepage' and element_name = 'shopping_guide_robot', device_id, null)) enter_clk_uv,
                0                                                                          button_expre_uv,
                count(distinct if(page_code = 'robert_guide_session' and element_name = 'robert_viewall_click', device_id,null))  button_clk_uv
         from dwd.dwd_vova_log_common_click a
         where pt = '${cur_date}'  and a.dp = 'vova'
     ) tmp
    ) e on a.list = e.list
left join (select 'all' list,count(distinct device_id) session_expre_uv from dwd.dwd_vova_log_page_view where pt = '${cur_date}' and page_code = 'robert_guide_session' and dp = 'vova') f
on a.list = f.list
"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

