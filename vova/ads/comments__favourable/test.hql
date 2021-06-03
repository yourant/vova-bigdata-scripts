create table tmp.ysj_20210602 as
select goods_id,
       buyer_id,
       comment_id,
       mct_id,
       0 as         first_cat_id,
       row_number() over(order by goods_num DESC) rank,
       0 as order_type
from (
         select distinct com.*,
                         sum(pay.goods_number) over(partition by com.goods_id  ) as goods_num
         from (
                  select goods_id,
                         buyer_id,
                         comment_id,
                         mct_id,
                         first_cat_id
                  from (
                           select goods_id,
                                  buyer_id,
                                  comment_id,
                                  mct_id,
                                  first_cat_id,
                                  post_time,
                                  row_number() over(partition by mct_id order by post_time DESC) rk
                           from (
                                    select c.goods_id,
                                           c.buyer_id,
                                           c.comment_id,
                                           c.mct_id,
                                           dg.first_cat_id,
                                           c.post_time,
                                           row_number() over(partition by c.goods_id order by c.post_time DESC) rk
                                    from dwd.dwd_vova_fact_comment c
                                             inner join dim.dim_vova_goods dg
                                                        on dg.goods_id = c.goods_id
                                                            and dg.is_on_sale = 1
                                    where c.rating = 5
                                      and length(split(c.comment, '<br><div')[0]) > 15
                                      and locate('<img', c.comment) > 0
                                ) t1
                           where t1.rk <= 3
                       ) t2
                  where t2.rk <= 15
                  order by post_time desc limit 1000
              ) com
                  left join dwd.dwd_vova_fact_pay pay
                            on pay.goods_id = com.goods_id
                                and datediff('2021-06-02', to_date(pay.pay_time)) <= 15
     ) t3
union all
select *
from (
         select goods_id,
                buyer_id,
                comment_id,
                mct_id,
                first_cat_id,
                row_number() over(partition by first_cat_id order by goods_num DESC) rank,
                0 as order_type
         from (
                  select distinct com.*,
                                  sum(pay.goods_number) over(partition by com.goods_id  ) as goods_num
                  from (
                           select goods_id,
                                  buyer_id,
                                  comment_id,
                                  mct_id,
                                  first_cat_id
                           from (
                                    select goods_id,
                                           buyer_id,
                                           comment_id,
                                           mct_id,
                                           first_cat_id,
                                           post_time,
                                           row_number() over(partition by mct_id order by post_time DESC) rk
                                    from (
                                             select c.goods_id,
                                                    c.buyer_id,
                                                    c.comment_id,
                                                    c.mct_id,
                                                    dg.first_cat_id,
                                                    c.post_time,
                                                    row_number() over(partition by c.goods_id order by c.post_time DESC) rk
                                             from dwd.dwd_vova_fact_comment c
                                                      inner join dim.dim_vova_goods dg
                                                                 on dg.goods_id = c.goods_id
                                                                     and dg.is_on_sale = 1
                                             where c.rating = 5
                                               and length(split(c.comment, '<br><div')[0]) > 15
                                               and locate('<img', c.comment) > 0
                                         ) t1
                                    where t1.rk <= 3
                                ) t2
                           where t2.rk <= 15
                           order by post_time desc
                       ) com
                           left join dwd.dwd_vova_fact_pay pay
                                     on pay.goods_id = com.goods_id
                                         and datediff('2021-06-02', to_date(pay.pay_time)) <= 15
              ) t3
     ) t4
where rank <= 1000
