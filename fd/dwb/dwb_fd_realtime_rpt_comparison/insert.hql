insert overwrite table dwb.dwb_fd_realtime_rpt_comparison partition (pt = '${pt}')
select project,
       platform,
       country,
       session_number,
       order_number,
       gmv,
       goods_amount,
       order_number / session_number               as conversion_rate,
       session_number_1d_ago,
       order_number_1d_ago,
       gmv_1d_ago,
       goods_amount_1d_ago,
       order_number_1d_ago / session_number_1d_ago as conversion_rate_1d_ago,
       session_number_7d_ago,
       order_number_7d_ago,
       gmv_7d_ago,
       goods_amount_7d_ago,
       order_number_7d_ago / session_number_7d_ago as conversion_rate_7d_ago

from (
         select project,
                platform,
                country,
                sum(if(pt = '${pt}', order_number, 0))                as order_number,
                sum(if(pt = '${pt}', session_number, 0))              as session_number,
                sum(if(pt = '${pt}', gmv, 0))                         as gmv,
                sum(if(pt = '${pt}', goods_amount, 0))                as goods_amount,

                sum(if(pt = date_sub('${pt}', 1), order_number, 0))   as order_number_1d_ago,
                sum(if(pt = date_sub('${pt}', 1), session_number, 0)) as session_number_1d_ago,
                sum(if(pt = date_sub('${pt}', 1), gmv, 0))            as gmv_1d_ago,
                sum(if(pt = date_sub('${pt}', 1), goods_amount, 0))   as goods_amount_1d_ago,

                sum(if(pt = date_sub('${pt}', 7), order_number, 0))   as order_number_7d_ago,
                sum(if(pt = date_sub('${pt}', 7), session_number, 0)) as session_number_7d_ago,
                sum(if(pt = date_sub('${pt}', 7), gmv, 0))            as gmv_7d_ago,
                sum(if(pt = date_sub('${pt}', 7), goods_amount, 0))   as goods_amount_7d_ago
         from dwb.dwb_fd_realtime_new_rpt
         where pt in ('${pt}', date_sub('${pt}', 1), date_sub('${pt}', 7))
           and hour <= ${hour}
         group by project,
                  platform,
                  country) t1