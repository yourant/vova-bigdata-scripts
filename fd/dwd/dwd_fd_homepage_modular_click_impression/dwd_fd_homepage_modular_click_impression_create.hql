create table if not exists dwd.dwd_fd_homepage_modular_click_impression
(   
    dt                                    string    comment 'UTC时间', 
    project                               string    comment '组织', 
    platform_type                         string    comment '平台', 
    country                               string    comment '国家', 
    is_new_user                           string    comment '新旧用户', 
    app_version                           string    comment 'app版本号',
    pv                                    bigint    comment '首页曝光pv',
    uv                                    bigint    comment '首页曝光pv去重',
    modular                               string    comment '模块', 
      
    impression_ss                         bigint    comment '曝光', 
    click_ss                              bigint    comment '点击',
    ctr                                   string    comment '点击/曝光',
    distinct_impression_ss                bigint    comment '去重曝光',
    distinct_click_ss                     bigint    comment '去重点击',
    uv_ctr                                string    comment '去重点击/去重曝光'             
)   
    comment "首页各模块曝光点击报表"
    PARTITIONED BY (`pt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS parquet
    TBLPROPERTIES ("parquet.compress"="SNAPPY");