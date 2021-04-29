create table if not exists dwd.dwd_fd_app_install
(
    device_id      string,
    project_name   string,
    country_code   string,
    platform_type  string,
    install_time   timestamp,
    install_source string,
    campaign       string,
    ga_channel     string,
    idfa           string,
    idfv           string,
    advertising_id string,
    imei           string,
    android_id     string
)
    partitioned by (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;