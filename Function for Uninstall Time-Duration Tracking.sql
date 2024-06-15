/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=2113146658
- Data: 
- Function: data_vajapora.fn_daily_uninstall_stats()
- Table: data_vajapora.daily_uninstall_stats
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_daily_uninstall_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : After how long users are uninstalling the app
Auxiliary data table(s) : data_vajapora.help_dev_id, data_vajapora.time_to_uninstall
Target data table(s)    : data_vajapora.daily_uninstall_stats
*/

declare

begin

	-- mobile_no and reg_datetime against device_ids
	drop table if exists data_vajapora.help_dev_id;
	create table data_vajapora.help_dev_id as
	select mobile_no, device_id, reg_datetime
	from 
		(select mobile_number mobile_no, created_at reg_datetime
		from public.register_usermobile
		) tbl1 
		
		inner join
	
		(select id, device_id, mobile mobile_no, created_at device_status_created_at, device_status
		from public.registered_users 
		) tbl2 using(mobile_no)
		
		inner join 
	
		(select mobile mobile_no, max(id) id
		from public.registered_users 
		group by 1
		) tbl3 using(mobile_no, id);
		
	-- time taken to uninstall: days, hours, mins, seconds
	drop table if exists data_vajapora.time_to_uninstall;
	create table data_vajapora.time_to_uninstall as
	select 
		*, 
		
		date_part('day', updated_at-created_at) days_to_uninstall, 
		
		date_part('day', updated_at-created_at)*24
		+date_part('hour', updated_at-created_at) 
		hours_to_uninstall,
		
		date_part('day', updated_at-created_at)*24*60
		+date_part('hour', updated_at-created_at)*60
		+date_part('minute', updated_at-created_at)
		mins_to_uninstall,
		
		date_part('day', updated_at-created_at)*24*60*60
		+date_part('hour', updated_at-created_at)*60*60
		+date_part('minute', updated_at-created_at)*60
		+date_part('second', updated_at-created_at)
		seconds_to_uninstall
	from 
		(select batch_id, device_id, created_at, updated_at, app_status
		from sync_operation.notification_fcmtoken
		where app_status='UNINSTALLED'
		) tbl1 
		
		inner join 
		
		(select device_id, min(batch_id) batch_id
		from sync_operation.notification_fcmtoken
		where app_status='UNINSTALLED'
		group by 1
		) tbl2 using(device_id, batch_id)
		
		left join 
		
		data_vajapora.help_dev_id tbl3 using(device_id)
	where updated_at>created_at; 
	
	-- daily categorized times to uninstall
	drop table if exists data_vajapora.daily_uninstall_stats;
	create table data_vajapora.daily_uninstall_stats as
	select 
		date(updated_at) uninstall_date, 
		
		count(distinct device_id) uninstalled,
		
		count(distinct case when days_to_uninstall=0 then device_id else null end) uninstalled_on_day_1,
		count(distinct case when days_to_uninstall in(1, 2) then device_id else null end) uninstalled_on_day_2_3,
		count(distinct case when days_to_uninstall in(3, 4, 5, 6) then device_id else null end) uninstalled_on_day_4_5_6_7,
		count(distinct case when days_to_uninstall>=7 then device_id else null end) uninstalled_after_7_days,
		
		count(distinct case when days_to_uninstall=0 then device_id else null end)*1.00/count(distinct device_id) uninstalled_on_day_1_pct,
		count(distinct case when days_to_uninstall in(1, 2) then device_id else null end)*1.00/count(distinct device_id) uninstalled_on_day_2_3_pct,
		count(distinct case when days_to_uninstall in(3, 4, 5, 6) then device_id else null end)*1.00/count(distinct device_id) uninstalled_on_day_4_5_6_7_pct,
		count(distinct case when days_to_uninstall>=7 then device_id else null end)*1.00/count(distinct device_id) uninstalled_after_7_days_pct
	from data_vajapora.time_to_uninstall
	group by 1
	order by 1; 

	-- drop auxiliary tables
	drop table if exists data_vajapora.help_dev_id;
	drop table if exists data_vajapora.time_to_uninstall;

END;
$function$
;

/*
select data_vajapora.fn_daily_uninstall_stats(); 

select *
from data_vajapora.daily_uninstall_stats; 
*/
