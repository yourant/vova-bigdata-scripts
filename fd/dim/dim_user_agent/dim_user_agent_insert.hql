INSERT overwrite table dim.dim_fd_user_agent
select 
    `user_agent_id`
    , case
          when is_app = 0 and device_type = 'pc' then 'PC'
          when is_app = 0 and device_type = 'mobile' then 'H5'
          when is_app = 0 and device_type = 'pad' then 'Tablet'
          when is_app = 1 and os_type = 'ios' then 'IOS'
          when is_app = 1 and os_type = 'android' then 'Android'
          else 'others'
    end           as platform
    , case
          when is_app = 0 and device_type = 'pc' then 'pc_web'
          when is_app = 0 and device_type = 'mobile' then 'mobile_web'
          when is_app = 0 and device_type = 'pad' then 'tablet_web'
          when is_app = 1 and os_type = 'ios' then 'ios_app'
          when is_app = 1 and os_type = 'android' then 'android_app'
          else 'others'
    end           as platform_type
    , `is_app`
    , `device_type`
    , `os_type`
    , `version`  as ua_version
    , `device_id`
    , `uuid`     as ua_uuid
    , `device_name`
    , `browser`
    , `idfa`
    , `idfv`
    , `imei`
    , `android_id`
    , `ga_id`
from ods_fd_vb.ods_fd_user_agent_analysis;
