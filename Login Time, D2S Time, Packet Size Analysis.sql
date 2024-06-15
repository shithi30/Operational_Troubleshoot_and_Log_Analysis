-- data, viz: https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=440346156 

-- login time
select 
	to_char(created_at, 'YYYY-MM-DD HH24') date_hour, 
	count(1) logins,
	count(case when next_total_time>=0 and next_total_time<=3 then 1 else null end) within_3_sec, 
	count(case when next_total_time>3 and next_total_time<=30 then 1 else null end) within_4_to_30_sec, 
	count(case when next_total_time>30 and next_total_time<=60 then 1 else null end) within_31_to_60_sec, 
	count(case when next_total_time>60 and next_total_time<=300 then 1 else null end) within_61_to_300_sec, 
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
		where event_name='user-login-api'
		) tbl1 
		
		inner join 
	
		(select id next_id, total_time next_total_time
		from public.eventapp_event
		) tbl2 using(next_id)
	where 
		message='request received'
		and next_message='response generated'
		and app_version::int>=118
	) tbl1  
group by 1;

-- D2S time
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
		and app_version::int>=118
	) tbl1 
	
	inner join 
	
	(select id, total_time 
	from public.eventapp_event
	) tbl2 using(id) 
group by 1 
order by 1; 

-- packet size
select 
	date_hour, 
	count(1) all_packets,
	count(case when len>0 and len<=1*1024 then 1 else null end) packet_size_within_1_kb,
	count(case when len>1*1024 and len<=5*1024 then 1 else null end) packet_size_2_to_5_kb,
	count(case when len>5*1024 and len<=10*1024 then 1 else null end) packet_size_6_to_10_kb,
	count(case when len>10*1024 and len<=15*1024 then 1 else null end) packet_size_11_to_15_kb,
	count(case when len>15*1024 and len<=20*1024 then 1 else null end) packet_size_16_to_20_kb,
	count(case when len>20*1024 then 1 else null end) packet_size_21_or_more_kb
from 
	(select 
		to_char(created_at, 'YYYY-MM-DD HH24') date_hour, 
		length(details) len,
		count(1) d2s
	from 
		(select *
		from data_vajapora.eventapp_event_temp
		where 
			event_name like 'device_to_server%' 
			and message like '%request%'
			and level='INFO'
			and app_version::int>=118
		) tbl1 
		
		inner join 
		
		(select id, details 
		from public.eventapp_event
		) tbl2 using(id)
	group by 1, 2
	) tbl1 
group by 1; 