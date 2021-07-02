CREATE TABLE IF NOT EXISTS dwb.dwb_fd_order_detail
(
     order_id               BIGINT,                                    
     order_time_utc         STRING,                                 
     pay_time_utc           STRING,                                      
     ga_channel             STRING,                                      
     is_pre_order           STRING,                                 
     bonus                  DECIMAL(5, 2),                                      
     project_name           STRING,                                      
     country_code           STRING,                                      
     pay_status             int,                                                                                                     
     virtual_goods_id       string,                                 
     goods_amount           DECIMAL(5, 2),                               
     goods_number           INT,                                                                 
     cat_id                 BIGINT,                                       
     goods_id               BIGINT,                                         
     cat_name               string,                                         
     platform_type          STRING                                                                                                    
)comment'替代artemis.order_detail'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUET;