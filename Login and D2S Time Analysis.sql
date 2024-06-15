/*
- Viz: 
- Data: 
	- https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=1923107454
	- https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=2005154352
	- https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=1082076424
	- https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=117727222
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

-- events
drop table if exists data_vajapora.eventapp_event_temp; 
create table data_vajapora.eventapp_event_temp as
select id, "level", event_name, message, user_id, created_at, app_version
from public.eventapp_event
where created_at>='16-Nov-22 00:00:00' and created_at<now(); 

-- D2S 
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
		and app_version::int=118
	) tbl1  
group by 1; 

/* misc. */

-- per user time
select 
	user_id, 
	app_version, 
	avg(next_total_time) avg_login_time
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
		from tallykhata.eventapp_event_temp
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
	) tbl1 
group by 1, 2
order by 3 desc; 

-- avg. time
select 
	to_char(created_at, 'YYYY-MM-DD hour: HH24') date_hr, 
	app_version, 
	avg(next_total_time) avg_login_time
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
		from tallykhata.eventapp_event_temp
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
	) tbl1 
group by 1, 2
order by 1, 2; 