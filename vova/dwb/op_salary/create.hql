drop table if exists dwb.dwb_vova_op_salary_thd;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_op_salary_thd
(
    first_cat_name        string        COMMENT '一级品类名称',
    sale_threshold_d      decimal(10,4)        COMMENT '日销额阈值'
) COMMENT '运营提成报表-提成标准'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table if exists dwb.dwb_vova_op_salary_goods_ok;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_op_salary_goods_ok
(
    first_cat_name        string        COMMENT '一级品类名称',
    group_id              bigint        COMMENT '分组id',
    goods_id              bigint        COMMENT '有效商品id',
    is_self               string        COMMENT '是否为自营店铺',
    op                    string        COMMENT '运营',
    mct_op                string        COMMENT '商家运营',
    ok_days               bigint        COMMENT '达标天数'
) COMMENT '运营提成报表-商品达标明细'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table if exists dwb.dwb_vova_op_salary_summary;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_op_salary_summary
(
    first_cat_name        string        COMMENT '一级品类名称',
    self_goods_cnt        bigint        COMMENT '自营店铺提成商品数',
    no_self_goods_cnt     bigint        COMMENT '非自营店铺提成商品数',
    op_amount             decimal(10,4) COMMENT '运营提成金额',
    mct_op_amount         decimal(10,4) COMMENT '商家运营提成金额'
) COMMENT '运营提成报表-提成汇总'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;









