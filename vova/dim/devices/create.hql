drop table tmp.tmp_vova_device_first_pay;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_device_first_pay
(
    datasource            string comment '数据平台',
    device_id        string COMMENT '设备ID',
    first_order_id   bigint COMMENT '设备支付首单ID',
    first_order_time timestamp COMMENT '设备支付首单下单时间',
    first_pay_time   timestamp COMMENT '设备支付首单支付时间'
) COMMENT '买家首单信息'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table tmp.tmp_vova_device_app_version;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_device_app_version
(
    datasource            string comment '数据平台',
    device_id     string COMMENT '设备ID',
    app_version   string COMMENT '当前device版本',
    buyer_id      bigint COMMENT '设备对应的买家ID',
    platform      string,
    region_code      string,
    app_region_code      string,
    activate_time timestamp COMMENT '设备激活时间'
) COMMENT '设备app版本信息'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table dim.dim_vova_devices;
CREATE TABLE IF NOT EXISTS dim.dim_vova_devices
(
    datasource          string comment '数据平台',
    device_id           string COMMENT '设备ID',
    main_channel        string COMMENT '主渠道',
    child_channel       string COMMENT '子渠道',
    platform            string COMMENT '设备平台',
    idfv                string COMMENT '设备标识',
    android_id          string COMMENT '设备标识',
    imei                string COMMENT '设备标识',
    advertising_id      string COMMENT '营销专用',
    http_referrer       string COMMENT '营销专用',
    campaign            string COMMENT '营销专用',
    os_version          string COMMENT '系统版本',
    device_brand        string COMMENT '设备品牌',
    device_model        string COMMENT '设备型号',
    region_code        string COMMENT '设备安装所在国',
    app_region_code        string COMMENT '设备安装所在国',
    language_code       string COMMENT '设备使用语言',
    install_time        timestamp COMMENT '设备安装时间',
    install_app_version string COMMENT '设备安装的app版本',
    current_app_version string COMMENT '设备当前的app版本',
    current_buyer_id    BIGINT COMMENT '设备对应的user_id',
    activate_time       timestamp COMMENT '设备激活时间',
    first_order_id      BIGINT COMMENT '设备支付的首单id',
    first_order_time    timestamp COMMENT '设备支付的首单下单时间',
    first_pay_time    timestamp COMMENT '设备支付的首单支付时间',
    clk_url           string COMMENT ''
) COMMENT '设备维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;







