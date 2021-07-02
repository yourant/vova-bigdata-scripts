drop table if exists dwb.dwb_vova_push_reject_appeal_info;
CREATE EXTERNAL TABLE dwb.dwb_vova_push_reject_appeal_info
(
    cur_date                string COMMENT '统计的日期',
    merchant_id                int COMMENT ' 商家id',
    reject_appeal_num                int COMMENT '驳回申诉数量',
    shipping_num                int COMMENT '发货数量',
    reject_appeal_rate                decimal(8, 2) COMMENT '驳回申诉占比 乘以100之后的'
)
    COMMENT '推送商家退款驳回申诉数据'
    PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;

