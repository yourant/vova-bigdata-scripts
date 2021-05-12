with
    gmv_and_paid_temp as
        (
            select
               to_date(from_utc_timestamp(from_unixtime(pay_time),'PRC')) pt
              ,lower(project_name) project
              ,sum(goods_amount+shipping_fee) GMV
              ,sum(goods_amount) sales_amount
              ,count(if(pay_status = '2',1,null)) paid_amount
              ,count(if(pay_status = '2' and platform_type in ('pc_web','tablet_web'),1,null)) PC_paid_amount
              ,count(if(pay_status = '2' and platform_type  = 'mobile_web',1,null)) M_paid_amount
              ,count(if(pay_status = '2' and platform_type  = 'ios_app',1,null)) IOS_paid_amount
              ,count(if(pay_status = '2' and platform_type  = 'android_app',1,null)) Android_paid_amount
            from
              dwd.dwd_fd_order_info
            where
              to_date(from_utc_timestamp(from_unixtime(pay_time),'PRC')) > '2020-03-01'
              and platform_type in ('pc_web','tablet_web','mobile_web','ios_app','android_app')
              and lower(project_name) = 'floryday'
            group by
               to_date(from_utc_timestamp(from_unixtime(pay_time),'PRC'))
              ,lower(project_name)
        ),

    dau_tmp as
        (
            select
               pt
              ,project
              ,DAU
              ,PC_DAU
              ,M_DAU
              ,IOS_DAU
              ,Android_DAU
            from
            (
              (select
                 pt
                ,lower(project) project
                ,count(distinct session_id) DAU
                ,count(distinct if(platform_type in ('pc_web','tablet_web'),session_id,null)) PC_DAU
                ,count(distinct if(platform_type  = 'mobile_web',session_id,null)) M_DAU
                ,count(distinct if(platform_type  = 'ios_app',session_id,null)) IOS_DAU
                ,count(distinct if(platform_type  = 'android_app',session_id,null)) Android_DAU
              from
                ods_fd_snowplow.ods_fd_snowplow_view_event
              where
                PT >= "2020-10-01"
                and platform_type in ('pc_web','tablet_web','mobile_web','ios_app','android_app')
                and lower(project) = 'floryday'
              group by
                 pt
                ,lower(project)
              )

            union all

              (select
                 pt
                ,lower(project) project
                ,count(distinct session_id) DAU
                ,count(distinct if(platform_type in ('pc_web','tablet_web'),session_id,null)) PC_DAU
                ,count(distinct if(platform_type  = 'mobile_web',session_id,null)) M_DAU
                ,count(distinct if(platform_type  = 'ios_app',session_id,null)) IOS_DAU
                ,count(distinct if(platform_type  = 'android_app',session_id,null)) Android_DAU
              from
                dwd.dwd_fd_sp_view_arc
              where
                pt between '2020-03-01' and "2020-10-01"
                and platform_type in ('pc_web','tablet_web','mobile_web','ios_app','android_app')
                and lower(project) = 'floryday'
              group by
                 pt
                ,lower(project)
              )
            )
        )

insert overwrite table dwb.dwb_fd_mail_floryday_YearOnYear partition(pt = '${pt}')
select
   /*+ REPARTITION(1) */
   tab1.date1
  ,tab1.project
  ,tab1.GMV
  ,tab1.sales_amount
  ,tab1.paid_amount
  ,tab2.DAU

  ,tab3.GMV
  ,tab3.sales_amount
  ,tab3.paid_amount
  ,tab4.DAU

  ,tab1.PC_paid_amount
  ,tab2.PC_DAU
  ,tab1.M_paid_amount
  ,tab2.M_DAU
  ,tab1.IOS_paid_amount
  ,tab2.IOS_DAU
  ,tab1.Android_paid_amount
  ,tab2.Android_DAU
from
(
    (
        select
           pt as date1
          ,project
          ,GMV
          ,sales_amount
          ,paid_amount
          ,PC_paid_amount
          ,M_paid_amount
          ,IOS_paid_amount
          ,Android_paid_amount
        from
          gmv_and_paid_temp
        where
          pt = '${pt}'
    )tab1

    left join

    (
        select
           pt as date1
          ,project
          ,DAU
          ,PC_DAU
          ,M_DAU
          ,IOS_DAU
          ,Android_DAU
        from
           dau_tmp
        where
           pt = '${pt}'
    )tab2
    on
       tab1.date1 = tab2.date1
       and tab1.project = tab2.project

    left join
    (
        select
           if(year(pt)%4 =0 and year(pt)%100 !=0 or year(pt)%400 = 0,date_add(pt,365),date_add(pt,364)) date1
          ,project
          ,GMV
          ,sales_amount
          ,paid_amount
        from
          gmv_and_paid_temp
        where
          pt = if(year('${pt}')%4 =0 and year('${pt}')%100 !=0 or year('${pt}')%400 = 0,date_sub('${pt}',366),date_sub('${pt}',365))
    ) tab3
    on
      tab1.date1 = tab3.date1
      and tab1.project = tab3.project

    left join
    (
        select
           if(year(pt)%4 =0 and year(pt)%100 !=0 or year(pt)%400 = 0,date_add(pt,365),date_add(pt,364)) date1
          ,project
          ,DAU
        from
           dau_tmp
        where
           pt = if(year('${pt}')%4 =0 and year('${pt}')%100 !=0 or year('${pt}')%400 = 0,date_sub('${pt}',366),date_sub('${pt}',365))
    )tab4
    on
      tab1.date1 = tab4.date1
      and tab1.project = tab4.project
)