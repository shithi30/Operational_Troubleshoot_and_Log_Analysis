/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=6348517
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

-- funnel 
select
	seq,
	event_name, 
	left(message, 28) message,
	count(1) events
from 
	(select *, row_number() over(partition by user_id order by id asc) seq
	from 
		(select distinct * from tallykhata.eventapp_event_temp) tbl1 
		inner join 
		(select mobile_number user_id
		from public.register_usermobile 
		where date(created_at)=current_date 
		) tbl2 using(user_id) 
	) tbl1 
where 
	   (event_name='/api/auth/init' and message='request received' and seq=1)
	or (event_name='login_v2' and message='valid request received' and seq=2)
	or (event_name='otp' and message='successfully sent' and seq=3)
	or (event_name='/api/auth/init' and message='response generated for new user' and seq=4)
	or (event_name='/api/auth/verify' and message='request received' and seq=5)
	or (event_name='/api/auth/verify' and message like 'Device is created with%' and seq=6)
	or (event_name='auth-verify-sign-up' and message like 'created new device with id:%' and seq=7)
	or (event_name='/api/auth/verify' and message='response generated for new user' and seq=8)
group by 1, 2, 3
order by 1;  

-- funnel data
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *
from 
	(select *, row_number() over(partition by user_id order by id asc) seq
	from 
		(select distinct * from tallykhata.eventapp_event_temp) tbl1 
		inner join 
		(select mobile_number user_id
		from public.register_usermobile 
		where date(created_at)=current_date 
		) tbl2 using(user_id) 
	) tbl1 
where 
	   (event_name='/api/auth/init' and message='request received' and seq=1)
	or (event_name='login_v2' and message='valid request received' and seq=2)
	or (event_name='otp' and message='successfully sent' and seq=3)
	or (event_name='/api/auth/init' and message='response generated for new user' and seq=4)
	or (event_name='/api/auth/verify' and message='request received' and seq=5)
	or (event_name='/api/auth/verify' and message like 'Device is created with%' and seq=6)
	or (event_name='auth-verify-sign-up' and message like 'created new device with id:%' and seq=7)
	or (event_name='/api/auth/verify' and message='response generated for new user' and seq=8)
order by 1;  

-- events against events
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select * 
from 
	(select user_id, seq seq_1
	from data_vajapora.help_a
	where seq=1
	) tbl1 
	
	left join 
	
	(select user_id, seq seq_2
	from data_vajapora.help_a
	where seq=2
	) tbl2 using(user_id)
	
	left join 
	
	(select user_id, seq seq_3
	from data_vajapora.help_a
	where seq=3
	) tbl3 using(user_id)
	
	left join 
	
	(select user_id, seq seq_4
	from data_vajapora.help_a
	where seq=4
	) tbl4 using(user_id)
	
	left join 
	
	(select user_id, seq seq_5
	from data_vajapora.help_a
	where seq=5
	) tbl5 using(user_id)
	
	left join 
	
	(select user_id, seq seq_6
	from data_vajapora.help_a
	where seq=6
	) tbl6 using(user_id)
	
	left join 
	
	(select user_id, seq seq_7
	from data_vajapora.help_a
	where seq=7
	) tbl7 using(user_id)
	
	left join 
	
	(select user_id, seq seq_8
	from data_vajapora.help_a
	where seq=8
	) tbl8 using(user_id) 
	
	left join 
	
	(select mobile_number user_id, date(created_at) reg_date 
	from public.register_usermobile
	) tbl9 using(user_id); 
	
-- 4 to 5 fall
select tbl2.*, tbl1.reg_date
from 
	data_vajapora.help_b tbl1 
	inner join 
	(select distinct * from tallykhata.eventapp_event_temp) tbl2 using(user_id)
where seq_4 is not null and seq_5 is null
order by user_id, id; 

-- 5 to 6 fall
select tbl2.*, tbl1.reg_date
from 
	data_vajapora.help_b tbl1 
	inner join 
	(select distinct * from tallykhata.eventapp_event_temp) tbl2 using(user_id)
where seq_5 is not null and seq_6 is null
order by user_id, id; 