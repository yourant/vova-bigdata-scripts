#!/bin/bash
sql="
insert overwrite table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,2 as cat_attr_id,a.second_cat_id from (
select goods_id,'band_material' as attr_key,band_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where band_material is not null
 union all
select goods_id,'battery_capacity' as attr_key,battery_capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where battery_capacity is not null
 union all
select goods_id,'battery_type' as attr_key,battery_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where battery_type is not null
 union all
select goods_id,'brand' as attr_key,brand as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where brand is not null
 union all
select goods_id,'case_material' as attr_key,case_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where case_material is not null
 union all
select goods_id,'clasp_type' as attr_key,clasp_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where clasp_type is not null
 union all
select goods_id,'cpu_manufacturer' as attr_key,cpu_manufacturer as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where cpu_manufacturer is not null
 union all
select goods_id,'cpu_model' as attr_key,cpu_model as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where cpu_model is not null
 union all
select goods_id,'detachable_wrist_strap' as attr_key,detachable_wrist_strap as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where detachable_wrist_strap is not null
 union all
select goods_id,'dial_diameter' as attr_key,dial_diameter as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where dial_diameter is not null
 union all
select goods_id,'dial_material' as attr_key,dial_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where dial_material is not null
 union all
select goods_id,'dial_thickness' as attr_key,dial_thickness as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where dial_thickness is not null
 union all
select goods_id,'dial_type' as attr_key,dial_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where dial_type is not null
 union all
select goods_id,'display_method' as attr_key,display_method as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where display_method is not null
 union all
select goods_id,'features' as attr_key,features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where features is not null
 union all
select goods_id,'for_people' as attr_key,for_people as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where for_people is not null
 union all
select goods_id,'gender' as attr_key,gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where gender is not null
 union all
select goods_id,'language' as attr_key,language as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where language is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where material is not null
 union all
select goods_id,'model' as attr_key,model as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where model is not null
 union all
select goods_id,'movement' as attr_key,movement as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where movement is not null
 union all
select goods_id,'multi_dial' as attr_key,multi_dial as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where multi_dial is not null
 union all
select goods_id,'network_mode' as attr_key,network_mode as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where network_mode is not null
 union all
select goods_id,'old_and_new' as attr_key,old_and_new as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where old_and_new is not null
 union all
select goods_id,'origin' as attr_key,origin as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where origin is not null
 union all
select goods_id,'product_diameter' as attr_key,product_diameter as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where product_diameter is not null
 union all
select goods_id,'product_height' as attr_key,product_height as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where product_height is not null
 union all
select goods_id,'product_length' as attr_key,product_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where product_length is not null
 union all
select goods_id,'product_type' as attr_key,product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where product_type is not null
 union all
select goods_id,'product_weight' as attr_key,product_weight as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where product_weight is not null
 union all
select goods_id,'product_width' as attr_key,product_width as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where product_width is not null
 union all
select goods_id,'ram' as attr_key,ram as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where ram is not null
 union all
select goods_id,'rear_camera' as attr_key,rear_camera as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where rear_camera is not null
 union all
select goods_id,'rom' as attr_key,rom as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where rom is not null
 union all
select goods_id,'screen_resolution' as attr_key,screen_resolution as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where screen_resolution is not null
 union all
select goods_id,'screen_size' as attr_key,screen_size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where screen_size is not null
 union all
select goods_id,'screen_type' as attr_key,screen_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where screen_type is not null
 union all
select goods_id,'size' as attr_key,size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where size is not null
 union all
select goods_id,'strap_length' as attr_key,strap_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where strap_length is not null
 union all
select goods_id,'strap_width' as attr_key,strap_width as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where strap_width is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where style is not null
 union all
select goods_id,'surface_shape' as attr_key,surface_shape as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where surface_shape is not null
 union all
select goods_id,'system' as attr_key,system as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where system is not null
 union all
select goods_id,'table_mirror_material' as attr_key,table_mirror_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where table_mirror_material is not null
 union all
select goods_id,'touch_screen' as attr_key,touch_screen as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where touch_screen is not null
 union all
select goods_id,'waterproof_depth' as attr_key,waterproof_depth as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where waterproof_depth is not null
 union all
select goods_id,'waterproof_level' as attr_key,waterproof_level as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where waterproof_level is not null
 union all
select goods_id,'whether_to_support_sim_card' as attr_key,whether_to_support_sim_card as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_watch where whether_to_support_sim_card is not null
) as a;



insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,3 as cat_attr_id,a.second_cat_id from (
	select goods_id,'accessories' as attr_key,accessories as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where accessories is not null
 union all
select goods_id,'applicable_area' as attr_key,applicable_area as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where applicable_area is not null
 union all
select goods_id,'applicable_holidays' as attr_key,applicable_holidays as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where applicable_holidays is not null
 union all
select goods_id,'applicable_location' as attr_key,applicable_location as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where applicable_location is not null
 union all
select goods_id,'applicable_parts' as attr_key,applicable_parts as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where applicable_parts is not null
 union all
select goods_id,'applicable_people' as attr_key,applicable_people as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where applicable_people is not null
 union all
select goods_id,'applicable_place' as attr_key,applicable_place as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where applicable_place is not null
 union all
select goods_id,'application' as attr_key,application as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where application is not null
 union all
select goods_id,'available_time' as attr_key,available_time as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where available_time is not null
 union all
select goods_id,'average_lifespan' as attr_key,average_lifespan as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where average_lifespan is not null
 union all
select goods_id,'base_type' as attr_key,base_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where base_type is not null
 union all
select goods_id,'battery_type' as attr_key,battery_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where battery_type is not null
 union all
select goods_id,'beam_angle' as attr_key,beam_angle as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where beam_angle is not null
 union all
select goods_id,'body_material' as attr_key,body_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where body_material is not null
 union all
select goods_id,'capacity' as attr_key,capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where capacity is not null
 union all
select goods_id,'certification' as attr_key,certification as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where certification is not null
 union all
select goods_id,'color' as attr_key,color as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where color is not null
 union all
select goods_id,'color_temperature' as attr_key,color_temperature as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where color_temperature is not null
 union all
select goods_id,'control_way' as attr_key,control_way as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where control_way is not null
 union all
select goods_id,'corrosion_resistance' as attr_key,corrosion_resistance as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where corrosion_resistance is not null
 union all
select goods_id,'fabric_type' as attr_key,fabric_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where fabric_type is not null
 union all
select goods_id,'feature' as attr_key,feature as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where feature is not null
 union all
select goods_id,'function' as attr_key,function as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where function is not null
 union all
select goods_id,'filling' as attr_key,filling as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where filling is not null
 union all
select goods_id,'fixed_types' as attr_key,fixed_types as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where fixed_types is not null
 union all
select goods_id,'flower_types' as attr_key,flower_types as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where flower_types is not null
 union all
select goods_id,'food_types' as attr_key,food_types as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where food_types is not null
 union all
select goods_id,'fruit_types' as attr_key,fruit_types as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where fruit_types is not null
 union all
select goods_id,'installation_method' as attr_key,installation_method as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where installation_method is not null
 union all
select goods_id,'ironing_boards_style' as attr_key,ironing_boards_style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where ironing_boards_style is not null
 union all
select goods_id,'is_batteries_included' as attr_key,is_batteries_included as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where is_batteries_included is not null
 union all
select goods_id,'is_batteries_required' as attr_key,is_batteries_required as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where is_batteries_required is not null
 union all
select goods_id,'is_bulbs_included' as attr_key,is_bulbs_included as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where is_bulbs_included is not null
 union all
select goods_id,'is_dimmable' as attr_key,is_dimmable as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where is_dimmable is not null
 union all
select goods_id,'is_smart' as attr_key,is_smart as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where is_smart is not null
 union all
select goods_id,'led_chip_model' as attr_key,led_chip_model as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where led_chip_model is not null
 union all
select goods_id,'led_lamp_bead_model' as attr_key,led_lamp_bead_model as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where led_lamp_bead_model is not null
 union all
select goods_id,'light_source' as attr_key,light_source as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where light_source is not null
 union all
select goods_id,'light_strip_style' as attr_key,light_strip_style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where light_strip_style is not null
 union all
select goods_id,'lighting_area' as attr_key,lighting_area as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where lighting_area is not null
 union all
select goods_id,'lighting_type' as attr_key,lighting_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where lighting_type is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where material is not null
 union all
select goods_id,'mop_type' as attr_key,mop_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where mop_type is not null
 union all
select goods_id,'mounting_method' as attr_key,mounting_method as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where mounting_method is not null
 union all
select goods_id,'movement_type' as attr_key,movement_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where movement_type is not null
 union all
select goods_id,'number_of_light_sources' as attr_key,number_of_light_sources as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where number_of_light_sources is not null
 union all
select goods_id,'number_of_sheets' as attr_key,number_of_sheets as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where number_of_sheets is not null
 union all
select goods_id,'placement' as attr_key,placement as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where placement is not null
 union all
select goods_id,'placement_method' as attr_key,placement_method as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where placement_method is not null
 union all
select goods_id,'plant_types' as attr_key,plant_types as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where plant_types is not null
 union all
select goods_id,'power' as attr_key,power as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where power is not null
 union all
select goods_id,'power_type' as attr_key,power_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where power_type is not null
 union all
select goods_id,'product_form' as attr_key,product_form as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where product_form is not null
 union all
select goods_id,'regional_characteristics' as attr_key,regional_characteristics as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where regional_characteristics is not null
 union all
select goods_id,'scale_type' as attr_key,scale_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where scale_type is not null
 union all
select goods_id,'screen_netting_material' as attr_key,screen_netting_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where screen_netting_material is not null
 union all
select goods_id,'season' as attr_key,season as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where season is not null
 union all
select goods_id,'shade_direction' as attr_key,shade_direction as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where shade_direction is not null
 union all
select goods_id,'shade_type' as attr_key,shade_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where shade_type is not null
 union all
select goods_id,'shape' as attr_key,shape as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where shape is not null
 union all
select goods_id,'size' as attr_key,size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where size is not null
 union all
select goods_id,'solar_cell_type' as attr_key,solar_cell_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where solar_cell_type is not null
 union all
select goods_id,'specification' as attr_key,specification as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where specification is not null
 union all
select goods_id,'start_way' as attr_key,start_way as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where start_way is not null
 union all
select goods_id,'structure' as attr_key,structure as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where structure is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where style is not null
 union all
select goods_id,'suitable_age' as attr_key,suitable_age as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where suitable_age is not null
 union all
select goods_id,'switch_type' as attr_key,switch_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where switch_type is not null
 union all
select goods_id,'switch_way' as attr_key,switch_way as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where switch_way is not null
 union all
select goods_id,'technics' as attr_key,technics as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where technics is not null
 union all
select goods_id,'thickness' as attr_key,thickness as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where thickness is not null
 union all
select goods_id,'thread_count' as attr_key,thread_count as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where thread_count is not null
 union all
select goods_id,'voltage' as attr_key,voltage as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where voltage is not null
 union all
select goods_id,'waterproof' as attr_key,waterproof as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where waterproof is not null
 union all
select goods_id,'weight' as attr_key,weight as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where weight is not null
 union all
select goods_id,'with_frame' as attr_key,with_frame as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where with_frame is not null
 union all
select goods_id,'with_lid' as attr_key,with_lid as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where with_lid is not null
 union all
select goods_id,'zoomable' as attr_key,zoomable as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_household where zoomable is not null
) as a;


insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,4 as cat_attr_id,a.second_cat_id from (
	select goods_id,'product_type' as attr_key,product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where product_type is not null
 union all
select goods_id,'gender' as attr_key,gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where gender is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where material is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where style is not null
 union all
select goods_id,'colour' as attr_key,colour as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where colour is not null
 union all
select goods_id,'product_features' as attr_key,product_features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where product_features is not null
 union all
select goods_id,'pattern_type' as attr_key,pattern_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where pattern_type is not null
 union all
select goods_id,'underwear_package' as attr_key,underwear_package as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where underwear_package is not null
 union all
select goods_id,'occasion' as attr_key,occasion as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where occasion is not null
 union all
select goods_id,'types_of_women_underwear' as attr_key,types_of_women_underwear as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where types_of_women_underwear is not null
 union all
select goods_id,'waist_type' as attr_key,waist_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where waist_type is not null
 union all
select goods_id,'decoration' as attr_key,decoration as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where decoration is not null
 union all
select goods_id,'size' as attr_key,size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where size is not null
 union all
select goods_id,'bra_styles' as attr_key,bra_styles as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where bra_styles is not null
 union all
select goods_id,'shoulder_strap_style' as attr_key,shoulder_strap_style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where shoulder_strap_style is not null
 union all
select goods_id,'season' as attr_key,season as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where season is not null
 union all
select goods_id,'petticoat_type' as attr_key,petticoat_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where petticoat_type is not null
 union all
select goods_id,'with_or_without_underwire' as attr_key,with_or_without_underwire as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where with_or_without_underwire is not null
 union all
select goods_id,'cup_shape' as attr_key,cup_shape as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where cup_shape is not null
 union all
select goods_id,'thickness' as attr_key,thickness as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where thickness is not null
 union all
select goods_id,'men_underwear' as attr_key,men_underwear as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where men_underwear is not null
 union all
select goods_id,'origin' as attr_key,origin as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where origin is not null
 union all
select goods_id,'color_style' as attr_key,color_style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_underwear where color_style is not null
) as a;


insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,5 as cat_attr_id,a.second_cat_id from (
	select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where material is not null
 union all
select goods_id,'compatible_brand' as attr_key,compatible_brand as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where compatible_brand is not null
 union all
select goods_id,'features' as attr_key,features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where features is not null
 union all
select goods_id,'type' as attr_key,type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where type is not null
 union all
select goods_id,'color' as attr_key,color as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where color is not null
 union all
select goods_id,'design' as attr_key,design as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where design is not null
 union all
select goods_id,'input_interface' as attr_key,input_interface as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where input_interface is not null
 union all
select goods_id,'cable_length' as attr_key,cable_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where cable_length is not null
 union all
select goods_id,'capacity' as attr_key,capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where capacity is not null
 union all
select goods_id,'maximum_current' as attr_key,maximum_current as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where maximum_current is not null
 union all
select goods_id,'power_connector_type' as attr_key,power_connector_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where power_connector_type is not null
 union all
select goods_id,'battery_capacity' as attr_key,battery_capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where battery_capacity is not null
 union all
select goods_id,'battery_type' as attr_key,battery_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where battery_type is not null
 union all
select goods_id,'max_output_power' as attr_key,max_output_power as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where max_output_power is not null
 union all
select goods_id,'cable_interface_type' as attr_key,cable_interface_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where cable_interface_type is not null
 union all
select goods_id,'certification' as attr_key,certification as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where certification is not null
 union all
select goods_id,'interface_type' as attr_key,interface_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where interface_type is not null
 union all
select goods_id,'quick_charge_protocol' as attr_key,quick_charge_protocol as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where quick_charge_protocol is not null
 union all
select goods_id,'origin' as attr_key,origin as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mobile where origin is not null
) as a;

insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,6 as cat_attr_id,a.second_cat_id from (
	select goods_id,'gender' as attr_key,gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where gender is not null
 union all
select goods_id,'shoes_type' as attr_key,shoes_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where shoes_type is not null
 union all
select goods_id,'occasion' as attr_key,occasion as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where occasion is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where style is not null
 union all
select goods_id,'feature' as attr_key,feature as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where feature is not null
 union all
select goods_id,'season' as attr_key,season as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where season is not null
 union all
select goods_id,'colour' as attr_key,colour as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where colour is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where material is not null
 union all
select goods_id,'upper_height' as attr_key,upper_height as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where upper_height is not null
 union all
select goods_id,'upper_material' as attr_key,upper_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where upper_material is not null
 union all
select goods_id,'closure_type' as attr_key,closure_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where closure_type is not null
 union all
select goods_id,'heel_height' as attr_key,heel_height as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where heel_height is not null
 union all
select goods_id,'fashion_element' as attr_key,fashion_element as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where fashion_element is not null
 union all
select goods_id,'insole_material' as attr_key,insole_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where insole_material is not null
 union all
select goods_id,'outsole_material' as attr_key,outsole_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where outsole_material is not null
 union all
select goods_id,'toe_shape' as attr_key,toe_shape as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where toe_shape is not null
 union all
select goods_id,'pattern_type' as attr_key,pattern_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where pattern_type is not null
 union all
select goods_id,'boot_height' as attr_key,boot_height as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where boot_height is not null
 union all
select goods_id,'heel_type' as attr_key,heel_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where heel_type is not null
 union all
select goods_id,'decorations' as attr_key,decorations as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where decorations is not null
 union all
select goods_id,'handmade' as attr_key,handmade as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where handmade is not null
 union all
select goods_id,'container_material' as attr_key,container_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where container_material is not null
 union all
select goods_id,'heel_material' as attr_key,heel_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_shoe where heel_material is not null
) as a;
"
spark-sql --conf "spark.app.name=ads_vova_goods_pre_attribute_data" --conf "spark.dynamicAllocation.maxExecutors=10" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi