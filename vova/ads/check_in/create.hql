drop table ads.ads_vova_check_in_d;
create
    external table if not exists ads.ads_vova_check_in_d
(
    event_date            date comment '日期',
    activate_range        string comment '激活时间',
    region_code           string comment '国家',
    gmv_stage             string comment '月卡分组',
    dau                   bigint comment '大盘DAU',
    check_in_uv           bigint comment '签到UV',
    order_rate            DECIMAL(15, 4) COMMENT '签到用户转化率',
    mission_give_check_in bigint comment '签到发放积分',
    mission_give_all      bigint comment '任务发放积分',
    lottery_cost          bigint comment '抽奖消耗积分',
    coupon_cost           bigint comment '优惠券消耗积分'
) COMMENT '积分签到整体数据表现' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

--ads.ads_vova_check_in_mission
drop table ads.ads_vova_check_in_mission;
create
    external table if not exists ads.ads_vova_check_in_mission
(
    activate_range                  string comment '激活时间',
    region_code                     string comment '国家',
    gmv_stage                       string comment '月卡分组',
    check_in_uv                     bigint comment '签到页UV',
    myCoinsTaskregiste_account_uv   bigint comment '完成注册任务UV',
    myCoinsTaskcomplete_address_uv  bigint COMMENT '完成填写地址UV',
    myCoinsTaskupdate_photo_uv      bigint comment '完成修改头像UV',
    myCoinsTaskreview_app_uv        bigint comment '完成评价AppUV',
    myCoinsTaskopen_notification_uv bigint comment '完成开启通知任务UV',
    myCoinsTaskcomplete_shopping_uv bigint comment '完成购买任务UV',
    mission_rate                    DECIMAL(15, 4) comment '任务参与率'
) COMMENT '积分签到任务完成情况' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table ads.ads_vova_check_in_retention;
create
    external table if not exists ads.ads_vova_check_in_retention
(
    region_code  string comment '国家',
    gmv_stage    string comment '月卡分组',
    check_in_uv  bigint comment '当日新激活用户签到页UV',
    retention_2  DECIMAL(15, 4) comment '第二天留存率',
    retention_3  DECIMAL(15, 4) comment '第三天留存率',
    retention_7  DECIMAL(15, 4) comment '第七天留存率',
    retention_14 DECIMAL(15, 4) comment '第十四天留存率',
    retention_30 DECIMAL(15, 4) comment '第三十天留存率'
) COMMENT '新激活用户留存看板' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table ads.ads_vova_check_in_game;
create
    external table if not exists ads.ads_vova_check_in_game
(
    activate_range                string comment '激活时间',
    region_code                   string comment '国家',
    gmv_stage                     string comment '月卡分组',
    check_in_uv                   bigint comment '签到页UV',
    check_in_game_uv              bigint comment '积分游戏UV',
    CoinsGameButtonNotInvolved_uv bigint comment '积分游戏参与UV',
    check_in_draw_uv              bigint comment '抽奖游戏页UV',
    freePlayButton_uv             bigint comment '免费抽奖UV',
    20PlayButton_uv               bigint comment '付费抽奖UV',
    20PlayButton_pv               bigint comment '付费抽奖PV',
    check_in_sport_uv             bigint comment '运动奖励页UV',
    sportsWelOpen_uv              bigint comment '运动开启UV',
    sportsWelExchange_uv          bigint comment '运动兑换UV'
) COMMENT '签到游戏参与情况' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table ads.ads_vova_check_in_sign;
create
    external table if not exists ads.ads_vova_check_in_sign
(
    region_code  string comment '国家',
    gmv_stage    string comment '月卡分组',
    sign_times_1 bigint comment '连续签到1天UV',
    sign_times_2 bigint comment '连续签到2天UV',
    sign_times_3 bigint comment '连续签到3天UV',
    sign_times_4 bigint comment '连续签到4天UV',
    sign_times_5 bigint comment '连续签到5天UV',
    sign_times_6 bigint comment '连续签到6天UV',
    sign_times_7 bigint comment '连续签到7天UV',
    sign_times_N bigint comment '连续签到7天+UV'
) COMMENT '连续签到看板' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


create
    external table if not exists tmp.tmp_check_in_dau
(
    activate_range string comment '激活时间',
    region_code    string comment '国家',
    gmv_stage      string comment '月卡分组',
    dau            bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_check_in_d_uv
(
    activate_range string comment '激活时间',
    region_code    string comment '国家',
    gmv_stage      string comment '月卡分组',
    check_in_uv    bigint,
    order_rate     DECIMAL(15, 4)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_check_in_distribution
(
    activate_range        string comment '激活时间',
    region_code           string comment '国家',
    gmv_stage             string comment '月卡分组',
    mission_give_check_in bigint,
    mission_give_all      bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_check_in_cost
(
    activate_range string comment '激活时间',
    region_code    string comment '国家',
    gmv_stage      string comment '月卡分组',
    lottery_cost   DECIMAL(15, 2),
    coupon_cost    DECIMAL(15, 2)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

--
create
    external table if not exists tmp.tmp_myCoinsTaskregiste_accountt
(
    device_id string,
    buyer_id  bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_myCoinsTaskcomplete_address
(
    device_id string,
    buyer_id  bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_myCoinsTaskupdate_photo
(
    device_id string,
    buyer_id  bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_myCoinsTaskreview_app
(
    device_id string,
    buyer_id  bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

    create
        external table if not exists tmp.tmp_myCoinsTaskopen_notification
    (
        device_id string,
        buyer_id  bigint
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_myCoinsTaskcomplete_shopping
(
    device_id string,
    buyer_id  bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_check_in_p_uv
(
    activate_range string comment '激活时间',
    region_code    string comment '国家',
    gmv_stage      string comment '月卡分组',
    check_in_uv    bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_myCoinsTaskcomplete_shopping_uv
(
    activate_range                  string comment '激活时间',
    region_code                     string comment '国家',
    gmv_stage                       string comment '月卡分组',
    myCoinsTaskcomplete_shopping_uv bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
--
create
    external table if not exists tmp.tmp_myCoinsTaskopen_notification_uv
(
    activate_range                  string comment '激活时间',
    region_code                     string comment '国家',
    gmv_stage                       string comment '月卡分组',
    myCoinsTaskopen_notification_uv bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_check_in_game_uv
(
    activate_range    string comment '激活时间',
    region_code       string comment '国家',
    gmv_stage         string comment '月卡分组',
    check_in_uv       bigint,
    check_in_game_uv  bigint,
    check_in_draw_uv  bigint,
    check_in_sport_uv bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_check_in_click_uv
(
    activate_range                string comment '激活时间',
    region_code                   string comment '国家',
    gmv_stage                     string comment '月卡分组',
    CoinsGameButtonNotInvolved_uv bigint,
    freePlayButton_uv             bigint,
    20PlayButton_uv               bigint,
    sportsWelOpen_uv              bigint,
    sportsWelExchange_uv          bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_check_in_click_pv
(
    activate_range  string comment '激活时间',
    region_code     string comment '国家',
    gmv_stage       string comment '月卡分组',
    20PlayButton_pv bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create
    external table if not exists tmp.tmp_check_in_new_device
(
    region_code string comment '国家',
    gmv_stage   string comment '月卡分组',
    device_id   string,
    pt          string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
