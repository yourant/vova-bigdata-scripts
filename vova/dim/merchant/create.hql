drop table dim.dim_vova_merchant;
CREATE TABLE IF NOT EXISTS dim.dim_vova_merchant
(
    datasource          string comment '数据平台',
    mct_id              bigint COMMENT '店铺ID',
    reg_time            timestamp COMMENT '店铺注册时间',
    mct_sn              string COMMENT '店铺所属sn',
    mct_name            string COMMENT '店铺英文名称',
    mct_name_cn         string COMMENT '店铺中文名称',
    mct_cat_desc        string COMMENT '店铺销售类别',
    sale_region_desc    string COMMENT '店铺销售销售范围',
    address             string COMMENT '店铺所在地区',
    account_type        string COMMENT '账户性质 公司还是个人',
    logistics_type_desc string COMMENT '店铺快递方式',
    reg_email           string COMMENT '注册时的邮箱',
    email               string COMMENT '现在的邮箱',
    phone               string COMMENT '电话',
    we_chat             string COMMENT '微信',
    qq                  string COMMENT 'QQ',
    is_delete           string COMMENT '是否被删除',
    is_banned           string COMMENT '是否禁售',
    is_on_vacation      string COMMENT '是否休假',
    review_status       string COMMENT '审核状态',
    mct_status          string COMMENT '店铺状态',
    first_publish_time  timestamp COMMENT '第一次发布商品的事件',
    tag                 string COMMENT '第一次发布商品12周以内的商家标签',
    first_customer_buy_time timestamp COMMENT '商家首次出单时间',
    settle_status       string COMMENT '是否可以结账',
    card_nbr       string COMMENT '身份证号',
    reg_ip       string COMMENT '注册ip',
    bank_nbr       array<string> COMMENT '银行卡号',
    paypal       array<string> COMMENT 'paypal',
    eqmnt       array<string> COMMENT '设备号',
    slr_name     string comment '卖家姓名',
    slr_addr     string comment '卖家地址',
    coupon_time  timestamp COMMENT '使用邀请码的时间',
    deposit_time  timestamp COMMENT '缴纳押金时间',
    spsor_name  string COMMENT '招商人姓名',
    pay_or_verify_time  timestamp COMMENT '缴纳押金/填写邀请码的时间'

) COMMENT '店铺维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

