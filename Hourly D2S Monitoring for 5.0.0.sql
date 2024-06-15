/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=0
	- https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=1743399016
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): D2S will fail if multiple requests (auto+manual) are placed concurrently. 
*/

-- D2S stats
select
	to_char(created_at, 'YYYY-MM-DD hour: HH24') date_hr, 
	
	count(distinct case when message like '%request%' and app_version::int>=112 then user_id end) as unique_user_new_version_d2s_request,
	count(distinct case when message like '%response%' and app_version::int>=112 then user_id end) as unique_user_new_version_d2s_request_successful,
	count(case when message like '%request%' and app_version::int>=112 then id end) as total_new_version_d2s_request,
	count(case when message like '%response%' and app_version::int>=112 then id end) as total_new_version_d2s_request_successful, 
	
	count(distinct case when message like '%request%' and not app_version::int>=112 then user_id end) as unique_user_old_version_d2s_request,
	count(distinct case when message like '%response%' and not app_version::int>=112 then user_id end) as unique_user_old_version_d2s_request_successful,
	count(case when message like '%request%' and not app_version::int>=112 then id end) as total_old_version_d2s_request,
	count(case when message like '%response%' and not app_version::int>=112 then id end) as total_old_version_d2s_request_successful
from tallykhata.eventapp_event_temp
where event_name like '%device_to_server%'
group by 1; 

-- users whose D2S failed
select 
	user_id, 
	count(case when message like '%request%' then id end) as total_new_version_d2s_request,
	count(case when message like '%response%' then id end) as total_new_version_d2s_request_successful
from tallykhata.eventapp_event_temp
where 
	event_name like '%device_to_server%'
	and app_version::int>=112
group by 1
having count(case when message like '%request%' then id end)!=count(case when message like '%response%' then id end); 

-- inv.
select * 
from tallykhata.eventapp_event_temp
where 
	event_name like '%device_to_server%'
	and app_version::int>=112
	and user_id in
	('01611895715', 
	'01719449056', 
	'01721440810', 
	'01745101532', 
	'01747958675', 
	'01753526068', 
	'01780775005', 
	'01793590348', 
	'01826805149', 
	'01832159684', 
	'01858133535', 
	'01911927691', 
	'01941791028', 
	'01943076744', 
	'01975876126', 
	'01980094330', 
	'01983615768') 
order by user_id, created_at; 

select * 
from 
	(-- detailed jsons
	select 
		*, 
		split_part(split_part(details, '''device_uuid'': ''', 2), '''', 1) device_id
	from public.eventapp_event
	where id in
		(select id
		from tallykhata.eventapp_event_temp
		where 
			event_name like '%device_to_server%'
			and app_version::int>=112
			and user_id in
			('01611895715', 
			'01719449056', 
			'01721440810', 
			'01745101532', 
			'01747958675', 
			'01753526068', 
			'01780775005', 
			'01793590348', 
			'01826805149', 
			'01832159684', 
			'01858133535', 
			'01911927691', 
			'01941791028', 
			'01943076744', 
			'01975876126', 
			'01980094330', 
			'01983615768')
		)
	) tbl1 
	
	left join 
	
	(-- active devices
	select mobile user_id, device_id active_device_id 
	from 
		(select mobile, max(id) id
		from public.registered_users 
		where device_status='active'
		group by 1 
		) tbl1 
		
		inner join 
		
		(select id, device_id 
		from public.registered_users 
		) tbl2 using(id) 
	) tbl2 using(user_id)
order by user_id, created_at; 

-- check in ledger: live
select * 
from public.ledger
where (mobile_no, device_id) in 
	(select 
		split_part(split_part(split_part(details , '(', 4), ')', 1), ', ', 1) mobile_no,
		split_part(split_part(split_part(details , '(', 4), ')', 1), ', ', 2)::int device_id
	from public.eventapp_event
	where 
		message='failed to save'
		and app_version::int>=112
		and user_id in
		('01611895715', 
		'01719449056', 
		'01721440810', 
		'01745101532', 
		'01747958675', 
		'01753526068', 
		'01780775005', 
		'01793590348', 
		'01826805149', 
		'01832159684', 
		'01858133535', 
		'01911927691', 
		'01941791028', 
		'01943076744', 
		'01975876126', 
		'01980094330', 
		'01983615768') 
	); 

-- inv. of all D2S cases for a single user: live
select * 
from 
	(-- detailed jsons
	select *, split_part(split_part(details, '''device_uuid'': ''', 2), '''', 1) device_id
	from public.eventapp_event
	where 
		event_name like '%device_to_server%'
		and app_version::int>=112
		and user_id in('01745101532')
	) tbl1 
	
	left join 
	
	(-- active devices
	select mobile user_id, device_id active_device_id 
	from 
		(select mobile, max(id) id
		from public.registered_users 
		where device_status='active'
		group by 1 
		) tbl1 
		
		inner join 
		
		(select id, device_id 
		from public.registered_users 
		) tbl2 using(id) 
	) tbl2 using(user_id)
order by user_id, id, created_at; 

-- overall
select 
	date_hr, 
	
	new_version_d2s_request, 
	new_version_d2s_request_successful, 
	
	tbl1.new_version_login_request+tbl2.new_version_login_request new_version_login_request,
	tbl1.new_version_successful_login_request+tbl2.new_version_successful_login_request new_version_successful_login_request,
	
	new_version_s2d_request,
	new_version_s2d_request_successful
from 
	(select
		to_char(created_at, 'YYYY-MM-DD hour: HH24') date_hr, 
		
		count(case when event_name like 'device_to_server%' and message like '%request%' then id end) as new_version_d2s_request,
		count(case when event_name like 'device_to_server%' and message like '%response%' then id end) as new_version_d2s_request_successful, 
		
		count(case when event_name='/api/auth/init' and message like 'response generated for%' then id end) as new_version_login_request,
		count(case when event_name='auth-verify-sign-in' then id end) as new_version_successful_login_request, 

		count(case when event_name like 'server_to_device%' and message like '%request%' then id end) as new_version_s2d_request,
		count(case when event_name like 'server_to_device%' and message like '%response%' then id end) as new_version_s2d_request_successful
	from systems_monitoring.event_table_temp
	where 
		app_version::int>111
		and date(created_at)>current_date-2 and date(created_at)<=current_date
	group by 1
	) tbl1 

	inner join 

	(select
		to_char(created_at, 'YYYY-MM-DD hour: HH24') date_hr,
		count(1) new_version_login_request, 
		count(case when next_message='response generated' then 1 else null end) new_version_successful_login_request
	from 
		(select 
			user_id,
			created_at, 
			app_version,
			event_name, 
			message, 
			lead(event_name, 1) over(partition by user_id order by id asc) next_event,
			lead(message, 1) over(partition by user_id order by id asc) next_message
		from systems_monitoring.event_table_temp
		where 
			event_name='user-login-api'
			and date(created_at)>current_date-2 and date(created_at)<=current_date
		) tbl1 
	where 
		message='request received'
		and app_version::int>111
	group by 1
	) tbl2 using(date_hr)
order by 1; 