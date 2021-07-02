create table if not exists dwb.dwb_fd_mail_floryday_YearOnYear
(
   date1                            string          COMMENT '数据日期'
  ,project                          string          COMMENT '站点'
  ,this_year_GMV                    decimal(15, 4)  COMMENT '今年成交总额'
  ,this_year_sales_amount           decimal(15, 4)  COMMENT '今年销售额'
  ,this_year_paid_amount            BIGINT          COMMENT '今年支付订单量'
  ,this_year_dau                    BIGINT          COMMENT '今年活跃人数'

  ,last_year_GMV                    decimal(15, 4)  COMMENT '去年成交总额'
  ,last_year_sales_amount           decimal(15, 4)  COMMENT '去年销售额'
  ,last_year_paid_amount            BIGINT          COMMENT '去年支付订单量'
  ,last_year_dau                    BIGINT          COMMENT '去年活跃人数'

  ,this_year_PC_sales_amount        decimal(15, 4)  COMMENT '今年pc销售额'
  ,this_year_PC_paid_amount         BIGINT          COMMENT '今年pc支付订单量'
  ,this_year_PC_dau                 BIGINT          COMMENT '今年pc活跃人数'

  ,this_year_M_sales_amount         decimal(15, 4)  COMMENT '今年H5销售额'
  ,this_year_M_paid_amount          BIGINT          COMMENT '今年H5支付订单量'
  ,this_year_M_dau                  BIGINT          COMMENT '今年m活跃人数'

  ,this_year_IOS_sales_amount       decimal(15, 4)  COMMENT '今年ios销售额'
  ,this_year_IOS_paid_amount        BIGINT          COMMENT '今年ios支付订单量'
  ,this_year_IOS_dau                BIGINT          COMMENT '今年ios活跃人数'

  ,this_year_Android_sales_amount   decimal(15, 4)  COMMENT '今年Android销售额'
  ,this_year_Android_paid_amount    BIGINT          COMMENT '今年Android支付订单量'
  ,this_year_Android_dau            BIGINT          COMMENT '今年Android活跃人数'
)COMMENT 'floryday同比报表'
    partitioned by (`pt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;