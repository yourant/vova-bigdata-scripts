-- mysql导出表的建表语句

create table IF NOT EXISTS income_cost_paid_rpt
(
    id            int(11) unsigned auto_increment comment '自增主键ID'
        primary key,
    project       varchar(32)    default ''                not null comment '网站名称',
    country_code  varchar(32)    default ''                not null comment '国家代码',
    country_name  varchar(64)    default ''                not null comment '国家名称',
    pt_date       varchar(16)    default '0000-00-00'      not null comment '数据对应日期',
    purchase_cost decimal(15, 4) default 0.000000          not null comment '预估采购花费',
    sale_amount   decimal(15, 4) default 0.000000          not null comment '已支付销售金额',
    coupon_cost   decimal(15, 4) default 0.000000          not null comment '优惠券花费',
    ads_cost      decimal(15, 4) default 0.000000          not null comment '广告花费',
    refund_cost   decimal(15, 4) default 0.000000          not null comment '退款花费',
    total_cost    decimal(15, 4) default 0.000000          not null comment '总花费',
    ctime         timestamp      default CURRENT_TIMESTAMP not null comment '创建时间类型字段',
    mtime         timestamp      default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP comment '更新时间类型字段',
    constraint uk_pt_prj_country
        unique (pt_date, project, country_code, country_name)
)
    comment '网站国家损益表已支付预估采购';