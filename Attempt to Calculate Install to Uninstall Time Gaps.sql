/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): Time from install to uninstall can't be calculated, since we have snapshot/changes from present, not from history/past. 
*/

-- reg info
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
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
	
-- uninstallation info
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *
from 
	(select device_id, created_at, updated_at, app_status, batch_id, id, app_version_number
	from sync_operation.notification_fcmtoken
	where app_status='UNINSTALLED'
	) tbl4 
	
	inner join 
	
	(select device_id, max(batch_id) batch_id
	from sync_operation.notification_fcmtoken 
	where app_status='UNINSTALLED'
	group by 1
	) tbl5 using(device_id, batch_id);

-- registration to uninstallation time
select *
from 
	(select 
		device_id, mobile_no, reg_datetime, created_at, updated_at,
	
		date_part('day', updated_at-reg_datetime) reg_to_uninstall_days, 
		
		date_part('day', updated_at-reg_datetime)*24
		+date_part('hour', updated_at-reg_datetime) 
		reg_to_uninstall_hours,
		
		date_part('day', updated_at-reg_datetime)*24*60
		+date_part('hour', updated_at-reg_datetime)*60
		+date_part('minute', updated_at-reg_datetime)
		reg_to_uninstall_mins,
		
		date_part('day', updated_at-reg_datetime)*24*60*60
		+date_part('hour', updated_at-reg_datetime)*60*60
		+date_part('minute', updated_at-reg_datetime)*60
		+date_part('second', updated_at-reg_datetime)
		reg_to_uninstall_seconds
	from 
		data_vajapora.help_a tbl1 
		inner join 
		data_vajapora.help_b tbl2 using(device_id)
	) tbl1
where 
	1=1
	and updated_at>reg_datetime
	and date(updated_at)>='2021-07-12'
	and reg_to_uninstall_mins<=20; 
