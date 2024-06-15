-- data, viz: https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=733701831

-- events
drop table if exists data_vajapora.eventapp_event_temp; 
create table data_vajapora.eventapp_event_temp as
select id, "level", event_name, message, user_id, created_at, app_version
from public.eventapp_event
where 
	created_at>=(current_date-interval '15 days')::timestamp and created_at<now()
	and event_name='user-login-api'; 

-- login times distrib. 
select 
	login_date, 
	case when successful_logins>10 then 'login more than 10 times' else concat('login ', successful_logins::text, ' times') end successful_logins_cat, 
	count(user_id) merchants_logged_in 
from 
	(select 
		date(created_at) login_date, 
		user_id, 
		count(1) successful_logins
	from 
		(select 
			app_version,
			user_id,
			created_at, 
			message, 
			lead(message, 1) over(partition by user_id order by id asc) next_message
		from data_vajapora.eventapp_event_temp
		) tbl1 
	where 
		message='request received'
		and next_message='response generated'
		and app_version::int>116 
	group by 1, 2
	) tbl1 
group by 1, 2
order by 1, 2;
