/*
- Viz: 
- Data: 
	- https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=0
	- https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=461820388
	- https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=488358691
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	I have investigated 4 people (1 with active, 3 with inactive devices) who faced 'inactive device' issues. 
	- For those who recently activated their devices, events are syncing but data are not. 
	- For users with inactive devices, neither data nor events are syncing. 
	
	For all cases, the following 4 APIs are called in this order:
	1. /api/v2/profile
	2. device_to_server_sync_v4
	3. sync_serializer (inactive device error)
	4. sync_app_event
	
	According to Mariam Apu, users may not be choosing the right device when prompted on login. 
*/

-- D2S requests not met with response
select * 
from 
	(select 
		id,
		user_id,
		created_at, 
		app_version,
		event_name, 
		message, 
		lead(event_name, 1) over(partition by user_id order by id asc) next_event,
		lead(message, 1) over(partition by user_id order by id asc) next_message
	from systems_monitoring.event_table_temp
	where 
		event_name like 'device_to_server%'
		and (message like '%request%' or message like '%response%')
		and date(created_at)=current_date
		and app_version::int=118
		and user_id!=''
	order by 2, 1
	) tbl1 
where 
	message like '%request%'
	and (next_message not like '%response%' or next_message is null); 

-- D2S requests met with (inactive device) error
select * 
from 
	(select 
		id,
		user_id,
		created_at, 
		app_version,
		event_name, 
		message, 
		lead(event_name, 1) over(partition by user_id order by id asc) next_event,
		lead(message, 1) over(partition by user_id order by id asc) next_message
	from systems_monitoring.event_table_temp
	where 
		date(created_at)=current_date
		and app_version::int=118
		and user_id!=''
	order by 2
	) tbl1 
where 1=1
	and event_name like 'device_to_server%'
	and next_event='sync_serializer'; 

-- users in new version with inactive devices
select distinct mobile mobile_no
from public.registered_users 
where 
	device_status!='active'
	and app_version_number=118;

-- D2S ERROR distrib.
select level, message, count(*) cases
from systems_monitoring.event_table_temp
where 
	date(created_at)=current_date
	and app_version::int=118
	and user_id!=''
	and event_name like 'device_to_server%'
	and level='ERROR'
group by 1, 2
order by 3 desc; 

-- 'failed to save' inv.
select * 
from systems_monitoring.event_table_temp
where 
	date(created_at)=current_date
	and app_version::int=118
	and event_name like 'device_to_server%'
	and user_id in
		(select user_id
		from systems_monitoring.event_table_temp
		where 
			date(created_at)=current_date
			and app_version::int=118
			and user_id!=''
			and message='failed to save'
		) 
order by user_id, id; 

-- seeif devices are really inactive

-- bring from live: data_vajapora.device_info
select distinct mobile user_id, device_status, updated_at device_status_updated_at
from public.registered_users 
where 
    app_version_number::int=118
    and device_status='active'; 

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select distinct user_id 
from systems_monitoring.event_table_temp 
where 
	event_name='sync_serializer' 
	and message='inactive device'
	and date(created_at)=current_date
	and app_version::int=118
	and user_id!=''; 

select * 
from 
	(select *
	from systems_monitoring.event_table_temp
	where 
		date(created_at)=current_date
		and app_version::int=118
		and user_id!=''
	) tbl1 
	
	inner join 
	
	data_vajapora.help_a tbl2 using(user_id)
	
	left join
	
	data_vajapora.device_info tbl3 using(user_id)
order by user_id, id; 