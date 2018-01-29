upsert into membership_info(
  pk_id,auth_id,driver_code,open_id,name,password,gender,mobile_phone,mail,birth_date,reg_time,zip,address,province,city,area,
  membership_type,user_level,org_id,agency_id,auth_kind,certificate_validity,certificate_address,driving_license,obtain_driver_timer,
  license_expiration_time,user_img_url,driving_license_img_url,identity_card_img_url,emergency_contact,emergency_mobil,guarantee_name,
  guarantee_ic_card_kind,guarantee_ic_card,guarantee_mobil,review_reason,review_status,card_no,credit_no,data_origin,id,status,
  blacklist_reason,review_user,review_items,illegal_method,deposit,reserve_amount,rent_mins,exempt_deposit,empno,info_origin,created_time,
  created_user,updated_time,updated_user,personnel_state,share_uid,ezbike,apply_status,mark,service_ver,service_ver_time,app_key,
  type_flag,invoiced_amount,channel_id,regionid,deposit_vehicle,province_of_origin,city_of_origin,hold_idcard_pic_url,point,
  idcard_pic_url,area_of_origin,review_time,review_remark)
select
  pk_id,auth_id,driver_code,open_id,name,password,gender,mobile_phone,mail,birth_date,reg_time,zip,address,province,city,area,
  cast(membership_type as tinyint),cast(user_level as tinyint),
  org_id,agency_id,auth_kind,certificate_validity,certificate_address,driving_license,obtain_driver_timer,license_expiration_time,
  user_img_url,driving_license_img_url,identity_card_img_url,emergency_contact,emergency_mobil,guarantee_name,
  cast(guarantee_ic_card_kind as tinyint),
  guarantee_ic_card,guarantee_mobil,review_reason,
  cast(review_status as int),
  card_no,credit_no,
  cast(data_origin as tinyint),
  id,
  cast(status as tinyint),
  blacklist_reason,review_user,review_items,
  cast(illegal_method as tinyint),
  deposit,reserve_amount,rent_mins,exempt_deposit,empno,info_origin,created_time,created_user,updated_time,updated_user,
  cast(personnel_state as tinyint),
  share_uid,
  cast(ezbike as tinyint),cast(apply_status as tinyint),
  mark,service_ver,service_ver_time,app_key,
  cast(type_flag as tinyint),
  invoiced_amount,channel_id,regionid,deposit_vehicle,province_of_origin,city_of_origin,hold_idcard_pic_url,
  cast(point as int),
  idcard_pic_url,area_of_origin,review_time,review_remark
from data03.membership_info;


upsert into order_info(
  order_seq,auth_id,org_id,card_no,vin,rent_method,guarantee_mobil,guarantee_name,guarantee_ic_card,guarantee_ic_card_kind,planpickupstoreseq,
  planpickupdatetime,planreturnstoreseq,planreturndatetime,pickupdatetime,returndatetime,pickup_store_seq,return_store_seq,return_mileage,
  get_mileage,purpose,pre_authorised_type,authorization_price,pay_trade_no,add_service_items,illegal_pre_authorised_flg,illegal_pre_authorised_amount,
  other_amount,pay_way,pre_pay_amount,other_amount_total,rent_total,amount,real_amount,exemption_amount,exemption_reason,compensate_amount,
  invoice_type,credit_no,payment_status,order_type,settlement_scan_url,check_car_url,rent_scan_url,return_soc,get_soc,bill_time,cost_time,
  exemption_time,return_remark,get_remark,sms_status,illegal_seq,remark,rent_shoper,account_shoper,origin,cancel_method,org_free,created_time,
  created_user,updated_time,updated_user,pay_item,cancel_flag,item_seq,vehicle_org_id,restrict_flat,restrict_content,return_error_code,
  bill_time_precharge,return_condition,abnormal_type,user_coupon_seq,discount,operation_org_id,location_type,isvehassess,pickveh_amount,
  banche_free,returnveh_amount,park_amount,unit_price,memdiscount,order_agency,activity_type,pay_time,pay_status_from_app,out_trade_seq,
  market_activity_type,deposit_deduct_amount)
select
  order_seq,auth_id,org_id,card_no,vin,
  cast(rent_method as tinyint),
  guarantee_mobil,guarantee_name,guarantee_ic_card,
  cast(guarantee_ic_card_kind as tinyint),
  planpickupstoreseq,planpickupdatetime,planreturnstoreseq,planreturndatetime,pickupdatetime,returndatetime,pickup_store_seq,return_store_seq,
  return_mileage,get_mileage,
  cast(purpose as tinyint),cast(pre_authorised_type as tinyint),
  authorization_price,pay_trade_no,add_service_items,
  cast(illegal_pre_authorised_flg as tinyint),
  illegal_pre_authorised_amount,other_amount,
  cast(pay_way as tinyint),
  pre_pay_amount,other_amount_total,rent_total,amount,real_amount,exemption_amount,exemption_reason,compensate_amount,
  cast(invoice_type as tinyint),
  credit_no,
  cast(payment_status as tinyint),cast(order_type as tinyint),
  settlement_scan_url,check_car_url,rent_scan_url,
  cast(return_soc as smallint),cast(get_soc as smallint),
  bill_time,cost_time,exemption_time,return_remark,get_remark,
  cast(sms_status as tinyint),
  illegal_seq,remark,rent_shoper,account_shoper,origin,
  cast(cancel_method as tinyint),cast(org_free as int),
  created_time,created_user,updated_time,updated_user,
  cast(pay_item as tinyint),cast(cancel_flag as tinyint),
  item_seq,vehicle_org_id,
  cast(restrict_flat as tinyint),
  restrict_content,return_error_code,bill_time_precharge,return_condition,
  cast(abnormal_type as tinyint),
  user_coupon_seq,discount,operation_org_id,
  cast(location_type as tinyint),cast(isvehassess as tinyint),
  pickveh_amount,
  cast(banche_free as tinyint),
  returnveh_amount,park_amount,unit_price,memdiscount,order_agency,
  cast(activity_type as int),
  pay_time,
  cast(pay_status_from_app as int),
  out_trade_seq,
  cast(market_activity_type as int),
  deposit_deduct_amount
from data03.order_info;



upsert into vehicle_info(
  vin,vehicle_no,vehicle_color,vehicle_name,org_id,owner_id,driver_id,vehicle_model_seq,engine_id,mileage,renttype,physics_status,service_status,
  vehicle_status,vehicle_nature,lock_status,return_soc,driving_range,returndatetime,sys_term_id,vehicle_body_color,trans_type,agency_type,rtpn,
  vehicle_nationality,trailer_vin,vehicle_orig_id,operation_org_id,product_date,purchase_date,last_check_date,next_check_date,insurance_date,
  local_car,shop_seq,price,status,remark,return_remark,get_remark,access_auth,created_user,created_time,updated_user,updated_time,onoffline_time,
  vehicleno_type,register_date,inspection_date,insurance_belongs,charging_status,disinfect_date,location_type,alarm_sim,battery_production_date,
  battery_sertal_int,dc_register_flg,monitor_flg,register_time,server_id,term_sn,vehicle_seq,service_flag,tci_startdate,tci_enddate,vci_startdate,
  vci_enddate,drivinglicense_img,tci_img,vci_img,record,is_test,tci_number,vci_number,stock_no,vehicle_no_task,time_share_task,long_rent_task,
  inspection_status,inspection_remark,insurance_belongs_renewal,tci_enddate_renewal,vci_enddate_renewal,tci_img_renewal,vci_img_renewal,
  drivinglicense_img_deputy,maintainance_date,last_simple_clean_time,insurace_flag,purchase_flag,contract_seq,assets_status,is_first_putin,
  drivinglicense_flag,inspection_flag,tci_label_flag,veh_agency_id)
select
  vin,vehicle_no,vehicle_color,vehicle_name,org_id,owner_id,driver_id,vehicle_model_seq,engine_id,mileage,
  cast(renttype as tinyint),cast(physics_status as tinyint),cast(service_status as tinyint),cast(vehicle_status as tinyint),
  cast(vehicle_nature as tinyint),cast(lock_status as tinyint),cast(return_soc as smallint),
  driving_range,returndatetime,sys_term_id,vehicle_body_color,trans_type,
  cast(agency_type as tinyint),
  rtpn,vehicle_nationality,trailer_vin,vehicle_orig_id,operation_org_id,product_date,purchase_date,last_check_date,next_check_date,insurance_date,
  local_car,shop_seq,price,status,remark,return_remark,get_remark,access_auth,created_user,created_time,updated_user,updated_time,onoffline_time,
  vehicleno_type,register_date,inspection_date,
  cast(insurance_belongs as int),
  charging_status,disinfect_date,
  cast(location_type as tinyint),
  alarm_sim,battery_production_date,battery_sertal_int,dc_register_flg,monitor_flg,register_time,server_id,term_sn,vehicle_seq,
  cast(service_flag as tinyint),
  tci_startdate,tci_enddate,vci_startdate,vci_enddate,drivinglicense_img,tci_img,vci_img,record,
  cast(is_test as tinyint),
  tci_number,vci_number,stock_no,vehicle_no_task,time_share_task,long_rent_task,inspection_status,inspection_remark,
  cast(insurance_belongs_renewal as int),
  tci_enddate_renewal,vci_enddate_renewal,tci_img_renewal,vci_img_renewal,drivinglicense_img_deputy,maintainance_date,last_simple_clean_time,
  insurace_flag,purchase_flag,contract_seq,assets_status,is_first_putin,drivinglicense_flag,inspection_flag,tci_label_flag,veh_agency_id
from data03.vehicle_info;


upsert into shop_info(
  shop_seq,shop_name,shop_manager_id,area_code,address,tel,fax,baidu_latitude,baidu_longitude,latitude,longitude,shop_pic_url,shop_logo_url,
  zip,charge_return_flag,shop_type,shop_property,stake_type,for_public,shop_close_time_normal,shop_close_time_weekly,shop_open_time_normal,
  shop_open_time_weekly,is_restrict,agency_id,org_id,remark,created_time,created_user,updated_time,updated_user,belonging_team,inspection_level,
  less_alarm_number,more_alarm_number,less_alarm_time,more_alarm_time,park_num,delete_flag,stack_num,pickveh_amount,charge_standards,
  returnveh_amount,park_amount,navigate_address,regionid,is_short_rent,bluetooth_flag,is_enable,parking_lock_number,shop_status,bd_id,
  acceptanc_time,grade,charge_amount,park_way_id,check_code,sim_code,face_id,shop_kind,isscancharge,rent_stake_num,rent_stake_amount,
  agency_amount,first_online_time,extra_org_code)
select
  cast(shop_seq as int),
  shop_name,shop_manager_id,area_code,address,tel,fax,baidu_latitude,baidu_longitude,latitude,longitude,shop_pic_url,shop_logo_url,zip,
  cast(charge_return_flag as tinyint),cast(shop_type as tinyint),cast(shop_property as tinyint),cast(stake_type as int),cast(for_public as tinyint),
  shop_close_time_normal,shop_close_time_weekly,shop_open_time_normal,shop_open_time_weekly,
  cast(is_restrict as tinyint),
  agency_id,org_id,remark,created_time,created_user,updated_time,updated_user,belonging_team,inspection_level,less_alarm_number,more_alarm_number,
  less_alarm_time,more_alarm_time,park_num,
  cast(delete_flag as tinyint),
  stack_num,pickveh_amount,charge_standards,returnveh_amount,park_amount,navigate_address,regionid,
  cast(is_short_rent as int),cast(bluetooth_flag as int),cast(is_enable as int),cast(parking_lock_number as int),cast(shop_status as int),
  bd_id,acceptanc_time,grade,charge_amount,
  cast(park_way_id as int),
  check_code,sim_code,face_id,
  cast(shop_kind as int),cast(isscancharge as int),cast(rent_stake_num as int),
  rent_stake_amount,agency_amount,first_online_time,extra_org_code
from data03.shop_info;


upsert into order_price_detail(
  id,order_seq,cross_district,reduce_cross_district,insurance,is_insurance,amount1,amount2,amount3,
  amount4,amount5,amount6,amount7,amount8,amount9,amount10,created_user,created_time,updated_user,updated_time)
select
  id,order_seq,cross_district,reduce_cross_district,insurance,
  cast(is_insurance as tinyint),
  amount1,amount2,amount3,amount4,amount5,amount6,amount7,amount8,amount9,amount10,created_user,created_time,updated_user,updated_time
from data03.order_price_detail;



upsert into agency_info(
  agency_id,agency_name,discount_inner,discount_outer,created_user,created_time,updated_user,updated_time,status,pickup_weeks,return_weeks,
  pickup_time,return_time,max_user_hour,exempt_deposit,contact,tel,mobile_phone,mail,license_no,fax,license_no_img_url,contract_img_url,
  tax_registration_img_url,org_code_img_url,remark,pay_way,deposit,rent_mins,check_date,check_alert,org_property,inside_flag,address,last_remind_date)
select
  agency_id,agency_name,discount_inner,discount_outer,created_user,created_time,updated_user,updated_time,
  cast(status as tinyint),
  pickup_weeks,return_weeks,pickup_time,return_time,max_user_hour,
  cast(exempt_deposit as int),
  contact,tel,mobile_phone,mail,license_no,fax,license_no_img_url,
  contract_img_url,tax_registration_img_url,org_code_img_url,remark,
  cast(pay_way as tinyint),
  deposit,rent_mins,check_date,
  cast(check_alert as int),cast(org_property as tinyint),cast(inside_flag as int),
  address,last_remind_date
from data03.agency_info;


upsert into iss_shop_fix(
  id,shop_seq,fix_type_id,brand_id,fix_count,req_man_id,fix_man_id,start_time,allot_time,fix_start_time,
  estimate_time,acturl_time,fix_duration,fix_amount,task_status,process_instance_id,fix_picture,before_fix_picture,
  after_fix_picture,fix_function,replace_count,amount_detail,fix_remark,fault_remark,fix_content,service_support,
  system_id,parent_id,misc_desc,status,create_time,create_oper_id,create_oper_name,update_time,update_oper_id,update_oper_name)
select
  id,
  cast(shop_seq as int),
  fix_type_id,brand_id,
  cast(fix_count as int),
  req_man_id,fix_man_id,start_time,allot_time,fix_start_time,estimate_time,acturl_time,fix_duration,fix_amount,
  cast(task_status as smallint),
  process_instance_id,fix_picture,before_fix_picture,after_fix_picture,
  cast(fix_function as smallint),cast(replace_count as smallint),
  amount_detail,fix_remark,fault_remark,fix_content,
  cast(service_support as smallint),cast(system_id as smallint),
  parent_id,misc_desc,
  cast(status as int),
  create_time,create_oper_id,create_oper_name,update_time,update_oper_id,update_oper_name
from data03.iss_shop_fix;


upsert into iss_fix_type(
  id,type_id,fix_type_name,org_id,use_status,misc_desc,status,
  create_time,create_oper_id,create_oper_name,update_time,update_oper_id,update_oper_name)
SELECT
  id,
  cast(type_id as smallint),
  fix_type_name,org_id,
  cast(use_status as smallint),
  misc_desc,
  cast(status as int),
  create_time,create_oper_id,create_oper_name,update_time,update_oper_id,update_oper_name
from data03.iss_fix_type;


upsert into vehicle_model(
  vehicle_model_seq,org_id,vehicle_model_id,vehicle_series_seq,vehicle_model_info,vehicle_class,vehicle_type,engine_type,
  engine_volume,mileage,oil_mileage,oil_type,approved_seats,approved_tonnge,tranction,vehilce_weight,maintain_time_interval,
  maintain_mileage_interval,sales_mode,vehicle_level,body_style,doors_num,body_pic_url,body_length,body_width,
  body_height,power_rate,torsion,battery_brand,battery_materia,electric_charge,dc_flag,ac_flag,auto_light,wheel_base,
  radar_flag,seat_material,tire_pressure_flag,one_start_flag,led_flag,authenticate_flag,ftpfile,ftpfilesize,
  charge_status_tag,rent_price,rent_unit,status,fas_status,remark,create_time,create_oper_id,create_oper_name,
  update_time,update_oper_id,update_oper_name,updated_time,updated_user,created_time,created_user,picture,deposit,
  grade,offline_flag,fuel,model_picture_big,vehicle_model_type)
select
  vehicle_model_seq,org_id,vehicle_model_id,vehicle_series_seq,vehicle_model_info,
  cast(vehicle_class as tinyint),
  vehicle_type,engine_type,engine_volume,
  cast(mileage as int),cast(oil_mileage as int),
  cast(oil_type as tinyint),cast(approved_seats as smallint),cast(approved_tonnge as smallint),
  cast(tranction as int),cast(vehilce_weight as int),cast(maintain_time_interval as smallint),
  cast(maintain_mileage_interval as smallint),cast(sales_mode as tinyint),cast(vehicle_level as tinyint),
  cast(body_style as tinyint),cast(doors_num as tinyint),
  body_pic_url,
  cast(body_length as smallint),cast(body_width as smallint),cast(body_height as smallint),
  power_rate,torsion,battery_brand,battery_materia,electric_charge,
  cast(dc_flag as tinyint),cast(ac_flag as tinyint),cast(auto_light as tinyint),
  cast(wheel_base as smallint),cast(radar_flag as tinyint),
  seat_material,
  cast(tire_pressure_flag as tinyint),cast(one_start_flag as tinyint),
  cast(led_flag as tinyint),cast(authenticate_flag as tinyint),
  ftpfile,ftpfilesize,charge_status_tag,rent_price,
  cast(rent_unit as int),cast(status as tinyint),cast(fas_status as tinyint),
  remark,create_time,create_oper_id,create_oper_name,update_time,update_oper_id,update_oper_name,
  updated_time,updated_user,created_time,created_user,picture,deposit,
  cast(grade as int),cast(offline_flag as int),cast(fuel as int),
  model_picture_big,
  cast(vehicle_model_type as int)
from data03.vehicle_model;


province
city
area

upsert into org_info(
  org_id,org_name,org_kind,org_class,contact,tel,mobile_phone,address,mail,license_no,
  fax,county,city,province,corporate,place,rtoln,license_no_img_url,tax_registration_img_url,
  org_code_img_url,remark,pay_way,created_user,created_time,updated_user,updated_time,
  status,origin,deposit,reserve_amount,rent_mins,agency_id,city_short,inside_flag,
  org_alias,check_date,check_alert,balance_mail,org_property,org_protrety)
SELECT
  org_id,org_name,org_kind,
  cast(org_class as tinyint),
  contact,tel,mobile_phone,address,mail,license_no,fax,county,city,province,corporate,
  `location`,rtoln,license_no_img_url,tax_registration_img_url,org_code_img_url,remark,
  cast(pay_way as tinyint),
  created_user,created_time,updated_user,updated_time,status,
  cast(origin as tinyint),
  deposit,reserve_amount,
  cast(rent_mins as int),
  agency_id,city_short,
  cast(inside_flag as int),
  org_alias,check_date,
  cast(check_alert as int),
  balance_mail,
  cast(org_property as tinyint),cast(org_protrety as tinyint)
from data03.org_info;


upsert into vehicle_offline_info(
	offline_seq,vin,status,off_time,recover_time,recover_user,off_type,remark,created_user,created_time,updated_user,updated_time
)
select
	offline_seq,vin,
	cast(status as tinyint),
	off_time,recover_time,recover_user,
	cast(off_type as tinyint),
	remark,created_user,created_time,updated_user,updated_time
from evcard_tmp.vehicle_offline_info;


upsert into assess_info(
	assess_seq,order_seq,options_assess,star_level,add_assess_info,imageurl_assess_info,assess_type,created_time,updated_time
)
select
	assess_seq,order_seq,options_assess,
	cast(star_level as tinyint),
	add_assess_info,imageurl_assess_info,
	cast(assess_type as tinyint),
	created_time,updated_time
from evcard_tmp.assess_info;


upsert into kafka_porter.mmp_credit_event_record (
  id,auth_id,order_seq,event_type_id,event_desc,event_source,event_name,event_file_path,
  event_image_path,event_nature,amount,black_list,misc_desc,status,create_time,update_time
)
SELECT
  id,
  auth_id,
  order_seq,
  event_type_id,
  event_desc,
  event_source,
  event_name,
  event_file_path,
  event_image_path,
  event_nature,
  cast(amount as int),
  cast(black_list as tinyint),
  misc_desc,
  cast(status as int),
  create_time,
  update_time
FROM


upsert into mmp_credit_event_type (
  id,event_name,event_nature,amount,event_desc,event_way,
  black_list,misc_desc,status,create_time,update_time
)
SELECT
  id,event_name,event_nature,
  cast(amount as int),
  event_desc,
  cast(event_way as int),cast(black_list as tinyint),
  misc_desc,
  cast(status as int),
  create_time,update_time
FROM data03.mmp_credit_event_type;


upsert into mmp_credit_event_type_report (
  id,year_num,org_id,`type`,event_type_id,event_name,
  `month`,total,misc_desc,status,create_time,update_time
)
SELECT
  id,
  year_num,
  org_id,
  cast(`type` as int),
  event_type_id,
  event_name,
  cast(`month` as int),
  cast(total as int),
  misc_desc,
  cast(status as int),
  create_time,
  update_time
FROM


upsert into kafka_porter.mmp_user_tag (
  id,auth_id,real_amount,effective_contd,remark,create_time,update_time,credit_amount
)
SELECT
  id,
  auth_id,
  real_amount,
  effective_contd,
  remark,
  create_time,
  update_time,
  credit_amount
FROM


upsert into kafka_porter.ids_dispatch_task (
  id,dispatch_task_seq,problem_type_id,region_id,province_id,city_id,area_id,place,auth_id,priority,
  order_seq,remark,dispatch_task_status,shop_seq,shop_name,vin,misc_desc,status,create_time,update_time,
  create_role,user_origin,vehicle_no,end_time,vehicle_model_seq,vehicle_model_info,report_time,
  report_reason,task_location,proc_inst_id,city_name,province_name,area_name,task_assign_time,
  task_cancel_time,region_name,related_task_seq,online_shop_name,start_time,org_id,no_restrictive_road_fee,
  restrictive_road_fee,park_fee,vehicle_allotment_num,vehicle_allotmented_num,vehicle_allotmenting_num,
  vehicle_allotment_shop_address,vehicle_allotment_shop_seq,vehicle_allotment_region,vehicle_allotment_region_id,
  vehicle_allotment_area,vehicle_allotment_area_id,vehicle_model_seq_list,vehicle_allotment_vehicle_no,
  vehicle_allotment_end_time,vehicle_allotment_shop_name,inspector_handle_result,is_setting_trouble,
  is_need_complete,clean_time_upper_limit,clean_time_lower_limit,clean_count,cleaning_count,
  cleaned_count,task_end_time,inspector_call_time,vehicle_operation_org_id
)
SELECT
  id,
  dispatch_task_seq,
  problem_type_id,
  region_id,
  province_id,
  city_id,
  area_id,
  place,
  auth_id,
  cast(priority as tinyint),
  order_seq,
  remark,
  cast(dispatch_task_status as int),
  cast(shop_seq as int),
  shop_name,
  vin,
  misc_desc,
  cast(status as int),
  create_time,
  update_time,
  create_role,
  user_origin,
  vehicle_no,
  end_time,
  vehicle_model_seq,
  vehicle_model_info,
  report_time,
  report_reason,
  task_location,
  proc_inst_id,
  city_name,
  province_name,
  area_name,
  task_assign_time,
  task_cancel_time,
  region_name,
  related_task_seq,
  online_shop_name,
  start_time,
  org_id,
  no_restrictive_road_fee,
  restrictive_road_fee,
  park_fee,
  vehicle_allotment_num,
  vehicle_allotmented_num,
  vehicle_allotmenting_num,
  vehicle_allotment_shop_address,
  vehicle_allotment_shop_seq,
  vehicle_allotment_region,
  vehicle_allotment_region_id,
  vehicle_allotment_area,
  vehicle_allotment_area_id,
  vehicle_model_seq_list,
  vehicle_allotment_vehicle_no,
  vehicle_allotment_end_time,
  vehicle_allotment_shop_name,
  inspector_handle_result,
  cast(is_setting_trouble as int),
  cast(is_need_complete as int),
  cast(clean_time_upper_limit as int),
  cast(clean_time_lower_limit as int),
  cast(clean_count as int),
  cast(cleaning_count as int),
  cast(cleaned_count as int),
  task_end_time,
  inspector_call_time,
  vehicle_operation_org_id
FROM

upsert into vehicle_dtc (
  dtc_seq,vin,trip_id,dtc,dtc_property,dtc_cnt,dtc_flag,dtc_time,
  status,remark,address,created_time,updated_time,dtc_class_id
)
SELECT
  cast(dtc_seq as int),
  vin,
  cast(trip_id as int),
  dtc,
  cast(dtc_property as int),cast(dtc_cnt as int),cast(dtc_flag as int),
  dtc_time,status,remark,address,created_time,updated_time,dtc_class_id
FROM data03.vehicle_dtc;


upsert into coupon_def(
  coupon_seq,rule_seq,coupon_type,org_id,time_type,start_time,end_time,min_amount,coupon_value,des,
  img_url_ios,img_url_android,pickshop_seq,img_url_ios_exp,img_url_android_exp,returnshop_seq,
  vehicle_modle,vehicle_no,service_type,valid_time_type,valid_days,start_date,expires_date,coupon_name,
  coupon_des,create_time,create_name,effective_days
)
SELECT
coupon_seq,
rule_seq,
cast(coupon_type as smallint),
org_id,
cast(time_type as int),
start_time,
end_time,
min_amount,
coupon_value,
des,
img_url_ios,
img_url_android,
pickshop_seq,
img_url_ios_exp,
img_url_android_exp,
returnshop_seq,
vehicle_modle,
vehicle_no,
cast(service_type as int),
cast(valid_time_type as int),
cast(valid_days as int),
start_date,
expires_date,
coupon_name,
coupon_des,
create_time,
create_name,
cast(effective_days as int)
FROM


upsert into user_coupon_list(
  user_coupon_seq,auth_id,coupon_seq,start_date,expires_date,status,created_time,created_user,updated_time,
  updated_user,coupon_origin,coupon_code,crm_user_coupon_seq,exchangetime,remark,offer_type,action_id
)
SELECT
user_coupon_seq,
auth_id,
coupon_seq,
start_date,
expires_date,
cast(status as tinyint),
created_time,
created_user,
updated_time,
updated_user,
coupon_origin,
coupon_code,
cast(crm_user_coupon_seq as int),
exchangetime,
remark,
cast(offer_type as int),
action_id
FROM


upsert into kafka_porter.app_key_manager(
  app_key,app_secret,request_app_key,plat_name,request_app_secret,post_url,class_name,org_id,remark,
  status,auto_regist,login_restrict,auto_pay,enjoy_benefit,upload_order,`type`,created_time,
  created_user,updated_time,updated_user
)
SELECT
app_key,
app_secret,
request_app_key,
plat_name,
request_app_secret,
post_url,
class_name,
org_id,
remark,
cast(status as tinyint),
cast(auto_regist as tinyint),
cast(login_restrict as tinyint),
cast(auto_pay as tinyint),
cast(enjoy_benefit as tinyint),
cast(upload_order as tinyint),
cast(`type` as tinyint),
created_time,
created_user,
updated_time,
updated_user
FROM
