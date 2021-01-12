CREATE TABLE `ads.ads_fd_income_cost`(
  `project` string COMMENT '网站名称',
  `country_code` string COMMENT '国家代码',
  `country_name` string COMMENT '国家名字',
  `dimension_type` string COMMENT '计算维度',
  `pt_date` string COMMENT '日期',
  `purchase_cost` decimal(15,4) COMMENT '购买花费',
  `sale_amount` decimal(15,4) COMMENT '销售金额',
  `coupon_cost` decimal(14,4) COMMENT '优惠券金额',
  `ads_cost` decimal(15,4) COMMENT '广告金额',
  `refund_cost` decimal(15,4) COMMENT '退款金额',
  `total_cost` decimal(15,4) COMMENT '总花费'
)
COMMENT '国家损益表'
PARTITIONED BY (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

--------sqoop mysql表-------
create table income_cost_rpt
(
    id             int(11) unsigned auto_increment comment '自增主键ID'
        primary key,
    project        varchar(32)    default ''                not null comment '网站名称',
    country_code   varchar(32)    default ''                not null comment '国家代码',
    country_name   varchar(64)    default ''                not null comment '国家名称',
    dimension_type varchar(32)    default ''                not null comment '时间维度',
    pt_date        varchar(16)    default '0000-00-00'      not null comment '数据对应日期',
    purchase_cost  decimal(15, 4) default 0.000000          not null comment '购买花费',
    sale_amount    decimal(15, 4) default 0.000000          not null comment '销售金额',
    coupon_cost    decimal(15, 4) default 0.000000          not null comment '优惠券花费',
    ads_cost       decimal(15, 4) default 0.000000          not null comment '广告花费',
    refund_cost    decimal(15, 4) default 0.000000          not null comment '退款花费',
    total_cost     decimal(15, 4) default 0.000000          not null comment '总花费',
    ctime          timestamp      default CURRENT_TIMESTAMP not null comment '创建时间类型字段',
    mtime          timestamp      default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP comment '更新时间类型字段',
    constraint uk_pt_prj_country_dim
        unique (pt_date, project, country_code, country_name, dimension_type)
)
    comment '网站国家损益表';