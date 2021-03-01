drop table dwb.dwb_vova_market_process;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_market_process
(
    event_date          date COMMENT '事件发生日期',
    datasource         string COMMENT '国家',
    region_code         string COMMENT '国家',
    tot_gmv             bigint COMMENT '当天gmv',
    tot_dau             bigint COMMENT '当天dau',
    tot_install         bigint COMMENT '当天安装设备数',
    android_paid_device bigint comment '当天支付设备数',
    android_gmv         bigint COMMENT '当天gmv',
    android_dau         bigint COMMENT '当天dau',
    android_install     bigint COMMENT '当天安装设备数',
    ios_paid_device     bigint comment '当天支付设备数',
    ios_gmv             bigint COMMENT '当天gmv',
    ios_dau             bigint COMMENT '当天dau',
    ios_install         bigint COMMENT '当天安装设备数',
    tot_android_1b_ret  bigint comment '全体用户当天启动设备1天前也启动的数量',
    tot_android_7b_ret  bigint comment '全体用户当天启动设备7天前也启动的数量',
    tot_android_28b_ret bigint comment '全体用户当天启动设备28天前也启动的数量',
    tot_ios_1b_ret      bigint comment '全体用户当天启动设备1天前也启动的数量',
    tot_ios_7b_ret      bigint comment '全体用户当天启动设备7天前也启动的数量',
    tot_ios_28b_ret     bigint comment '全体用户当天启动设备28天前也启动的数量',
    new_android_1b_ret  bigint comment '新激活用户当天启动设备1天前也启动的数量',
    new_android_7b_ret  bigint comment '新激活用户当天启动设备7天前也启动的数量',
    new_android_28b_ret bigint comment '新激活用户当天启动设备28天前也启动的数量',
    new_ios_1b_ret      bigint comment '新激活用户当天启动设备1天前也启动的数量',
    new_ios_7b_ret      bigint comment '新激活用户当天启动设备7天前也启动的数量',
    new_ios_28b_ret     bigint comment '新激活用户当天启动设备28天前也启动的数量',
    tot_activate     bigint comment '当天新激活数',
    android_activate     bigint comment '当天新激活数',
    ios_activate     bigint comment '当天新激活数',
    tot_lucky_gmv     bigint comment '一元夺宝gmv'
) COMMENT '大盘报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table dwb.dwb_vova_market;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_market
(
    event_date          date COMMENT '事件发生日期',
    datasource          string COMMENT 'datasource',
    region_code         string COMMENT '国家',
    tot_lucky_gmv       bigint COMMENT '一元夺宝 gmv',
    tot_gmv             bigint COMMENT '当天gmv',
    tot_dau             bigint COMMENT '当天dau',
    tot_install         bigint COMMENT '当天安装设备数',
    android_paid_device bigint comment '当天支付设备数',
    android_gmv         bigint COMMENT '当天gmv',
    android_dau         bigint COMMENT '当天dau',
    android_install     bigint COMMENT '当天安装设备数',
    ios_paid_device     bigint comment '当天支付设备数',
    ios_gmv             bigint COMMENT '当天gmv',
    ios_dau             bigint COMMENT '当天dau',
    ios_install         bigint COMMENT '当天安装设备数',
    tot_android_1b_ret  bigint comment '全体用户当天启动设备1天前也启动的数量',
    tot_android_7b_ret  bigint comment '全体用户当天启动设备7天前也启动的数量',
    tot_android_28b_ret bigint comment '全体用户当天启动设备28天前也启动的数量',
    tot_ios_1b_ret      bigint comment '全体用户当天启动设备1天前也启动的数量',
    tot_ios_7b_ret      bigint comment '全体用户当天启动设备7天前也启动的数量',
    tot_ios_28b_ret     bigint comment '全体用户当天启动设备28天前也启动的数量',
    new_android_1b_ret  bigint comment '新激活用户当天启动设备1天前也启动的数量',
    new_android_7b_ret  bigint comment '新激活用户当天启动设备7天前也启动的数量',
    new_android_28b_ret bigint comment '新激活用户当天启动设备28天前也启动的数量',
    new_ios_1b_ret      bigint comment '新激活用户当天启动设备1天前也启动的数量',
    new_ios_7b_ret      bigint comment '新激活用户当天启动设备7天前也启动的数量',
    new_ios_28b_ret     bigint comment '新激活用户当天启动设备28天前也启动的数量',
    tot_activate     bigint comment '当天新激活数',
    android_activate     bigint comment '当天新激活数',
    ios_activate     bigint comment '当天新激活数',
    new_android_1b_activate  bigint comment '新激活1天前用户数',
    new_android_7b_activate  bigint comment '新激活7天前用户数',
    new_android_28b_activate  bigint comment '新激活28天前用户数',
    new_ios_1b_activate  bigint comment '新激活1天前用户数',
    new_ios_7b_activate  bigint comment '新激活7天前用户数',
    new_ios_28b_activate  bigint comment '新激活28天前用户数',
    tot_android_1b_activate  bigint comment '1天前用户数',
    tot_android_7b_activate  bigint comment '天前用户数',
    tot_android_28b_activate  bigint comment '28天前用户数',
    tot_ios_1b_activate  bigint comment '1天前用户数',
    tot_ios_7b_activate  bigint comment '7天前用户数',
    tot_ios_28b_activate  bigint comment '28天前用户数'
) COMMENT '大盘报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



CREATE TABLE IF NOT EXISTS dwb.dwb_vova_market_web_dau
(
    event_date  date,
    datasource  string,
    region_code string,
    dau         bigint
) COMMENT 'web_dau' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


DROP TABLE dwb.dwb_vova_market_cb_region_filter;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_market_cb_region_filter
(
    datasource  string COMMENT '数据平台',
    event_date  date COMMENT '事件发生日期',
    region_code string COMMENT '国家',
    cr          DECIMAL(10, 4) COMMENT 'cr'
) COMMENT 'dwb_vova_market_cb_region_filter'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_market_cb;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_market_cb
(
    event_date          date COMMENT '事件发生日期',
    datasource         string COMMENT 'datasource',
    region_code         string COMMENT '国家',
    tot_gmv             bigint COMMENT '当天gmv',
    tot_dau             bigint COMMENT '当天dau',
    tot_install         bigint COMMENT '当天安装设备数',
    android_paid_device bigint comment '当天支付设备数',
    android_gmv         bigint COMMENT '当天gmv',
    android_dau         bigint COMMENT '当天dau',
    android_install     bigint COMMENT '当天安装设备数',
    ios_paid_device     bigint comment '当天支付设备数',
    ios_gmv             bigint COMMENT '当天gmv',
    ios_dau             bigint COMMENT '当天dau',
    ios_install         bigint COMMENT '当天安装设备数',
    tot_android_1b_ret  bigint comment '全体用户当天启动设备1天前也启动的数量',
    tot_android_7b_ret  bigint comment '全体用户当天启动设备7天前也启动的数量',
    tot_android_28b_ret bigint comment '全体用户当天启动设备28天前也启动的数量',
    tot_ios_1b_ret      bigint comment '全体用户当天启动设备1天前也启动的数量',
    tot_ios_7b_ret      bigint comment '全体用户当天启动设备7天前也启动的数量',
    tot_ios_28b_ret     bigint comment '全体用户当天启动设备28天前也启动的数量',
    new_android_1b_ret  bigint comment '新激活用户当天启动设备1天前也启动的数量',
    new_android_7b_ret  bigint comment '新激活用户当天启动设备7天前也启动的数量',
    new_android_28b_ret bigint comment '新激活用户当天启动设备28天前也启动的数量',
    new_ios_1b_ret      bigint comment '新激活用户当天启动设备1天前也启动的数量',
    new_ios_7b_ret      bigint comment '新激活用户当天启动设备7天前也启动的数量',
    new_ios_28b_ret     bigint comment '新激活用户当天启动设备28天前也启动的数量',
    tot_activate        bigint comment '当天新激活数',
    android_activate    bigint comment '当天新激活数',
    ios_activate        bigint comment '当天新激活数'
) COMMENT 'dwb_vova_market_cb' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_market_web_dau
(
    event_date  date,
    datasource  string,
    region_code string,
    dau         bigint
) COMMENT 'web_dau' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
;

#mysql

CREATE TABLE themis_logistics_report.dwb_vova_market_web_dau
(
    `event_date`  date        NOT NULL COMMENT '事件发生日期',
    `datasource`  varchar(20) NOT NULL DEFAULT '',
    `region_code` varchar(10) NOT NULL DEFAULT '',
    `dau`         int(11)              DEFAULT '0',
    PRIMARY KEY (`event_date`, `datasource`, `region_code`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='dwb_vova_market_web_dau'
;
insert into
themis_logistics_report.dwb_vova_market_web_dau
SELECT
*
from
themis_logistics_report.rpt_market_web_dau

CREATE TABLE themis_logistics_report.dwb_vova_market_process
(
    `event_date`          date        NOT NULL COMMENT '事件发生日期',
    `region_code`         varchar(20) NOT NULL DEFAULT '' COMMENT 'region_code',
    `datasource`          varchar(20) NOT NULL DEFAULT '',
    `tot_gmv`             int(11)              DEFAULT NULL COMMENT '当天gmv',
    `tot_dau`             int(11)              DEFAULT NULL COMMENT '当天dau',
    `tot_install`         int(11)              DEFAULT NULL COMMENT '当天安装设备数',
    `android_paid_device` int(11)              DEFAULT NULL COMMENT '当天支付设备数',
    `android_gmv`         int(11)              DEFAULT NULL COMMENT '当天gmv',
    `android_dau`         int(11)              DEFAULT NULL COMMENT '当天dau',
    `android_install`     int(11)              DEFAULT NULL COMMENT '当天安装设备数',
    `ios_paid_device`     int(11)              DEFAULT NULL COMMENT '当天支付设备数',
    `ios_gmv`             int(11)              DEFAULT NULL COMMENT '当天gmv',
    `ios_dau`             int(11)              DEFAULT NULL COMMENT '当天dau',
    `ios_install`         int(11)              DEFAULT NULL COMMENT '当天安装设备数',
    `tot_android_1b_ret`  int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备1天前也启动的数量',
    `tot_android_7b_ret`  int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备7天前也启动的数量',
    `tot_android_28b_ret` int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备28天前也启动的数量',
    `tot_ios_1b_ret`      int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备1天前也启动的数量',
    `tot_ios_7b_ret`      int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备7天前也启动的数量',
    `tot_ios_28b_ret`     int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备28天前也启动的数量',
    `new_android_1b_ret`  int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备1天前也启动的数量',
    `new_android_7b_ret`  int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备7天前也启动的数量',
    `new_android_28b_ret` int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备28天前也启动的数量',
    `new_ios_1b_ret`      int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备1天前也启动的数量',
    `new_ios_7b_ret`      int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备7天前也启动的数量',
    `new_ios_28b_ret`     int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备28天前也启动的数量',
    `tot_activate`        int(11)              DEFAULT NULL COMMENT '当天新激活数',
    `android_activate`    int(11)              DEFAULT NULL COMMENT '当天新激活数',
    `ios_activate`        int(11)              DEFAULT NULL COMMENT '当天新激活数',
    `tot_lucky_gmv`       int(11)              DEFAULT NULL COMMENT '当天gmv',
    PRIMARY KEY (`event_date`, `region_code`, `datasource`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='大盘报表'
;

CREATE TABLE themis_logistics_report.dwb_vova_market_cb
(
    `event_date`          date        NOT NULL COMMENT '事件发生日期',
    `region_code`         varchar(20) NOT NULL DEFAULT '' COMMENT 'region_code',
    `datasource`          varchar(20) NOT NULL DEFAULT '',
    `tot_gmv`             int(11)              DEFAULT NULL COMMENT '当天gmv',
    `tot_dau`             int(11)              DEFAULT NULL COMMENT '当天dau',
    `tot_install`         int(11)              DEFAULT NULL COMMENT '当天安装设备数',
    `android_paid_device` int(11)              DEFAULT NULL COMMENT '当天支付设备数',
    `android_gmv`         int(11)              DEFAULT NULL COMMENT '当天gmv',
    `android_dau`         int(11)              DEFAULT NULL COMMENT '当天dau',
    `android_install`     int(11)              DEFAULT NULL COMMENT '当天安装设备数',
    `ios_paid_device`     int(11)              DEFAULT NULL COMMENT '当天支付设备数',
    `ios_gmv`             int(11)              DEFAULT NULL COMMENT '当天gmv',
    `ios_dau`             int(11)              DEFAULT NULL COMMENT '当天dau',
    `ios_install`         int(11)              DEFAULT NULL COMMENT '当天安装设备数',
    `tot_android_1b_ret`  int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备1天前也启动的数量',
    `tot_android_7b_ret`  int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备7天前也启动的数量',
    `tot_android_28b_ret` int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备28天前也启动的数量',
    `tot_ios_1b_ret`      int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备1天前也启动的数量',
    `tot_ios_7b_ret`      int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备7天前也启动的数量',
    `tot_ios_28b_ret`     int(11)              DEFAULT NULL COMMENT '全体用户当天启动设备28天前也启动的数量',
    `new_android_1b_ret`  int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备1天前也启动的数量',
    `new_android_7b_ret`  int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备7天前也启动的数量',
    `new_android_28b_ret` int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备28天前也启动的数量',
    `new_ios_1b_ret`      int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备1天前也启动的数量',
    `new_ios_7b_ret`      int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备7天前也启动的数量',
    `new_ios_28b_ret`     int(11)              DEFAULT NULL COMMENT '新激活用户当天启动设备28天前也启动的数量',
    `tot_activate`        int(11)              DEFAULT NULL COMMENT '当天新激活数',
    `android_activate`    int(11)              DEFAULT NULL COMMENT '当天新激活数',
    `ios_activate`        int(11)              DEFAULT NULL COMMENT '当天新激活数',
    PRIMARY KEY (`event_date`, `region_code`, `datasource`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='大盘报表'
;

