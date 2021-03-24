

drop table if exists dwb.dwb_vova_stay_coupon;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_stay_coupon
(
    cur_time           string,
    coupon_id         string,
    coupon_cnt        int
) COMMENT '留存红包'
    PARTITIONED BY (pt string);
