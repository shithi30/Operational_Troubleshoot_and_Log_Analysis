/*
- Viz: https://docs.google.com/spreadsheets/d/1oRJHPauEZVmYWOrRBg3RCwo2suGunckmrjP48QdWyF4/edit#gid=0
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	1. Bhai, can you please provide these data?
	2. users activated from inactive state (after 4.0)
	Conversion rate comparison of unverified to verified users (1 month before and after 4.0 launch)
*/

-- 2.
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select *
from 
	(select mobile mobile_no, date(min(created_at)) entry_date_as_unverified
	from public.register_unverifieduserapp 
	group by 1
	) tbl1 
	
	left join 
	
	(select mobile_number mobile_no, date(created_at) verified_reg_date
	from public.register_usermobile 
	) tbl2 using(mobile_no); 

select *
from 
	(select verified_reg_date report_date, count(mobile_no) unverified_to_verified_merchants
	from data_vajapora.help_a 
	where verified_reg_date>=current_date-60
	group by 1
	) tbl1 
	
	inner join 
	
	(select entry_date_as_unverified report_date, count(mobile_no) merchants_entered_unverified
	from data_vajapora.help_a 
	where entry_date_as_unverified>=current_date-60
	group by 1
	) tbl2 using(report_date)
order by 1; 

-- 1. 
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select distinct mobile, device_id, created_at, device_status, updated_at
from public.registered_users; 
	
select *
from 
	(select mobile, max(created_at) latest_active_device_created_at
	from data_vajapora.help_a 
	where 
		device_status='active'
		and updated_at>='2021-09-29'
	group by 1
	) tbl1 
	
	inner join 
	
	(select mobile, max(created_at) latest_device_created_at
	from data_vajapora.help_a 
	group by 1
	) tbl2 using(mobile)
where latest_active_device_created_at<latest_device_created_at; 
