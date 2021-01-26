DROP TABLE IF EXISTS dwb.dwb_vova_newsletter_callback_email_detail;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_newsletter_callback_email_detail(
    `datasource`        string          COMMENT 'd_datasource',
    `nl_id`             BIGINT          COMMENT 'd_邮件id',
    `nl_title`          STRING          COMMENT 'd_名称',
    `create_time`       TIMESTAMP       COMMENT 'i_创建时间',
    `send_time`         TIMESTAMP       COMMENT 'i_发送时间',
    `lag_id`            STRING          COMMENT 'i_语言id',
    `email_dau`         BIGINT          COMMENT 'i_覆盖用户量',
    `email_send_cnt`    BIGINT          COMMENT 'i_邮件发送数量',
    `email_send_succ_cnt` BIGINT        COMMENT 'i_邮件成功发送数量',
    `email_open_cnt`    BIGINT          COMMENT 'i_邮件打开量',
    `buyer_callback`    BIGINT          COMMENT 'i_回归用户数',
    `gmv_1d`            DECIMAL(13,2)   COMMENT 'i_1日gmv',
    `gmv_3d`            DECIMAL(13,2)   COMMENT 'i_3日gmv',
    `gmv_7d`            DECIMAL(13,2)   COMMENT 'i_7日gmv'

) COMMENT 'newsletter召回邮件详情表' STORED AS PARQUETFILE;


DROP TABLE IF EXISTS dwb.dwb_vova_newsletter_callback_order_detail;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_newsletter_callback_order_detail(
    `datasource`        string          COMMENT 'd_datasource',
    `nl_id`             BIGINT          COMMENT 'd_邮件id',
    `nl_title`          STRING          COMMENT 'd_名称',
    `send_time`         TIMESTAMP       COMMENT 'i_发送时间',
    `platform`          STRING          COMMENT 'i_p平台',
    `ctry`              STRING          COMMENT 'i_国家id',
    `back_uv`    BIGINT          COMMENT 'i_回归用户数',
    `order_cnt`         BIGINT          COMMENT '订单量',
    `pay_cnt`           BIGINT          COMMENT '支付订单量',
    `pay_uv`            BIGINT          COMMENT '支付UV',
    `gmv_1h`            DECIMAL(13,2)   COMMENT 'i_1小时gmv',
    `gmv_3h`            DECIMAL(13,2)   COMMENT 'i_3小时gmv',
    `gmv_1d`            DECIMAL(13,2)   COMMENT 'i_1日gmv',
    `gmv_3d`            DECIMAL(13,2)   COMMENT 'i_3日gmv',
    `gmv_7d`            DECIMAL(13,2)   COMMENT 'i_7日gmv'

) COMMENT 'newsletter召回订单详情表' STORED AS PARQUETFILE;


DROP TABLE IF EXISTS dwb.dwb_vova_newsletter_callback_order_day;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_newsletter_callback_order_day(
    `datasource`        string          COMMENT 'd_datasource',
    `platform`          STRING          COMMENT 'd_平台',
    `ctry`              STRING          COMMENT 'd_国家',
    `back_uv`           BIGINT          COMMENT 'i_召回uv',
    `ord_cnt`           BIGINT          COMMENT 'i_订单量',
    `pay_cnt`           BIGINT          COMMENT 'i_支付订单量',
    `pay_uv`            BIGINT          COMMENT 'i_支付uv',
    `gmv`               DECIMAL(13,2)   COMMENT 'gmv'
) COMMENT 'newsletter召回订单详情每日表' partitioned by(pt string) STORED AS PARQUETFILE;