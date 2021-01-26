drop table if exists dwb.dwb_vova_buyer_partition;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_buyer_partition
(
    event_date                  string COMMENT 'd_日期',
    buyer_scope                 string COMMENT 'd_用户范围',
    reg_tag                     string COMMENT 'd_注册间隔',
    buyer_act                   string COMMENT 'd_用户活跃度',
    trade_act                   string COMMENT 'd_交易阶段',
    cnt                         bigint COMMENT 'i_用户数量',
    total_cnt                   bigint COMMENT 'i_总用户数',
    gmv                         DECIMAL(14, 4) COMMENT 'i_gmv',
    payed_uv                    bigint COMMENT 'i_支付uv',
    total_gmv                   DECIMAL(14, 4) COMMENT 'i_总gmv',
    cart_uv                     bigint COMMENT 'i_加车uv'
) COMMENT '用户分层报表'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



CREATE TABLE `rpt_buyer_partition` (
  `event_date` datetime NOT NULL,
  `buyer_scope` varchar(16)  NOT NULL DEFAULT '',
  `reg_tag` varchar(16)  NOT NULL DEFAULT '',
  `buyer_act` varchar(16) NOT NULL DEFAULT '',
  `trade_act` varchar(16) NOT NULL DEFAULT '',
  `cnt` int(11) NOT NULL DEFAULT '0',
  `total_cnt` int(11) NOT NULL DEFAULT '0',
  `gmv` decimal (10,2) NOT NULL DEFAULT '0',
  `payed_uv` int (11) NOT NULL DEFAULT '0',
  `total_gmv` decimal (10,2) NOT NULL DEFAULT '0',
  `cart_uv` int (11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`buyer_scope`,`event_date`,`reg_tag`,`buyer_act`,`trade_act`),
  KEY `event_date` (`event_date`),
  KEY `buyer_scope` (`buyer_scope`),
  KEY `reg_tag` (`reg_tag`),
  KEY `buyer_act` (`buyer_act`),
  KEY `trade_act` (`trade_act`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 comment '用户分层报表';