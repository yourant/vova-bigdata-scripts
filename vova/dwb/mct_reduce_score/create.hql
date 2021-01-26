-- 扣分商家历史数据
drop table dwb.dwb_vova_mct_reduce_score_history;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_mct_reduce_score_history
(
    mct_name                        string          COMMENT 'd_商家id',
    red_score                       int             COMMENT 'i_最近10天累计扣分',
    today_red_score                 int             COMMENT 'i_本日扣分',
    gs_sale_cnt                     bigint          COMMENT 'i_最近10日销售额',
    gmv                             decimal(13,2)   COMMENT 'i_最近10日gmv'
) COMMENT '扣分商家报表'
PARTITIONED BY ( pt string)
STORED AS PARQUETFILE;