-- D2S 

-- hourly, version wise, timeframe wise
select
	to_char(created_at, 'YYYY-MM-DD HH24') date_hour, 
	count(1) d2s,
	count(case when total_time>=0 and total_time<=3 then 1 else null end) within_3_sec, 
	count(case when total_time>3 and total_time<=30 then 1 else null end) within_30_sec, 
	count(case when total_time>30 and total_time<=60 then 1 else null end) within_60_sec, 
	count(case when total_time>60 and total_time<=300 then 1 else null end) within_300_sec, 
	count(case when total_time>300 then 1 else null end) morethan_300_sec
from 
	(select *
	from data_vajapora.eventapp_event_temp
	where 
		event_name like 'device_to_server%' 
		and message like '%response%'
		and level='INFO'
		and app_version::int=118
		and date(created_at)=current_date
	) tbl1 
	
	inner join 
	
	(select id, total_time 
	from public.eventapp_event
	) tbl2 using(id) 
group by 1 
order by 1; 

-- login 

-- hourly, version wise, timeframe wise
select 
	to_char(created_at, 'YYYY-MM-DD HH24') date_hour, 
	count(1) logins,
	count(case when next_total_time>=0 and next_total_time<=3 then 1 else null end) within_3_sec, 
	count(case when next_total_time>3 and next_total_time<=30 then 1 else null end) within_30_sec, 
	count(case when next_total_time>30 and next_total_time<=60 then 1 else null end) within_60_sec, 
	count(case when next_total_time>60 and next_total_time<=300 then 1 else null end) within_300_sec, 
	count(case when next_total_time>300 then 1 else null end) morethan_300_sec
from 
	(select 
		*, 
		 date_part('hour', next_created_at-created_at)*3600
		+date_part('minute', next_created_at-created_at)*60
		+date_part('second', next_created_at-created_at) login_sec
	from 
		(select 
			app_version,
			user_id,
			created_at, 
			message, 
			lead(message, 1) over(partition by user_id order by id asc) next_message, 
			lead(created_at, 1) over(partition by user_id order by id asc) next_created_at, 
			lead(id, 1) over(partition by user_id order by id asc) next_id
		from data_vajapora.eventapp_event_temp
		where 
			event_name='user-login-api'
			and date(created_at)=current_date
		) tbl1 
		
		inner join 
	
		(select id next_id, total_time next_total_time
		from public.eventapp_event
		) tbl2 using(next_id)
	where 
		message='request received'
		and next_message='response generated'
		and app_version::int=116
	) tbl1  
group by 1; 