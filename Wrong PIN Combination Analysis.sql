/*
- Viz: https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=1844960966
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
	1. How may users made mistake in selecting the right combination of PIN (# wrong attempt with non-compliant PIN)?
	- 10 users, 14 attempts
	- PIN already used: 1 user
	- PIN too simple: 9 users
		
	2. How many users came through the reset path?
	- 242 users
		
	3. Any failed case in reset through face verification?
	- 1 user, 9 cases
		
	4. #Login failed due to wrong PIN in TallyKhata (unique users count)?
	- 25 users, 61 attempts
*/

-- 1. How may users made mistake in selecting the right combination of PIN (# wrong attempt with non-compliant PIN)?
	
-- how many merchants try PIN reset daily
-- how many merchants fail at first attempt

select 
	date(created_at) report_date, 
	count(distinct user_id) merchants_attempted_pinreset, 
	count(distinct case when next_level='ERROR' then user_id else null end) merchants_failed_pinrest_first_attempt
from 
	(select 
		user_id, event_name, level, message, id, created_at, details, 
		lead(level, 1) over(partition by user_id order by id) next_level, 
		row_number() over(partition by user_id order by id asc) seq
	from test.eventapp_event_last_7days
	where event_name='check-pin-validity-api'
	) tbl1 
where seq=1
group by 1
order by 1; 
		
select 
	date(created_at) report_date, 
	trim('"' from (details::json->'message')::text) first_attempt_fail_reason, 
	count(id) first_attempt_failed_attempts
from 
	(select 
		user_id, event_name, level, message, id, created_at, details, 
		lead(level, 1) over(partition by user_id order by id) next_level, 
		row_number() over(partition by user_id order by id asc) seq
	from test.eventapp_event_last_7days
	where event_name='check-pin-validity-api'
	) tbl1 
where 
	seq=2 
	and level='ERROR'
group by 1, 2
order by 1, 2; 

-- 4. #Login failed due to wrong PIN in TallyKhata (unique users count)?
select date(created_at) report_date, fail_reason, count(distinct user_id) users, count(user_id) attempts 
from 
	(select *, trim('"' from (details::json->'message')::text) fail_reason
	from test.eventapp_event_last_7days
	where
		event_name like '%login%'
		and level='ERROR'
	) tbl1
where fail_reason in 
	('Invalid credentials',
	'You entered wrong PIN 2 times. Your account will be blocked if you enter wrong PIN 1 more time.',
	'Dear valued TallyPay user, for entering wrong PIN 3 times, your account has been blocked for 1 hour. Please try to login later.'
	)
group by 1, 2
order by 1, 2;

-- 3. Any failed case in reset through face verification?
select date(created_at) report_date, count(distinct user_id) face_verify_failed_users, count(id) face_verify_failed_cases
from test.eventapp_event_last_7days
where 
	level='ERROR' 
	and event_name='face-image-verify'
	and message='Error Occurred'
group by 1 
order by 1; 

-- from NP DWH
-- 2. How many users came through the reset path?
select date(created_at) report_date, count(id) reset_requests, count(distinct mobile_no) users_through_reset
from 
	(select *, trim('"' from (long_text::json->'wallet_no')::text) mobile_no
	from nobopay_api_gw.activity_log
	where url in('/api/portal/user/pin/reset')
	) tbl1 
where date(created_at)>=current_date-8 and date(created_at)<current_date-1
group by 1 
order by 1; 

/* Version-02 */

-- 1. How may users made mistake in selecting the right combination of PIN (# wrong attempt with non-compliant PIN)?
	
-- how many merchants try PIN reset daily
-- how many merchants fail at first attempt

select 
	date(created_at) report_date, 
	count(distinct user_id) merchants_attempted_pinreset, 
	count(distinct case when next_level='ERROR' then user_id else null end) merchants_failed_pinrest_first_attempt
from 
	(select 
		user_id, event_name, level, message, id, created_at, details, 
		lead(level, 1) over(partition by user_id order by id) next_level, 
		row_number() over(partition by user_id order by id asc) seq
	from public.eventapp_event
	where id in 
		(select id
		from systems_monitoring.event_table_temp
		where 
			event_name='check-pin-validity-api'
			and date(created_at)>=current_date-7 and date(created_at)<current_date
		)
	) tbl1 
where seq=1
group by 1
order by 1; 

select 
	date(created_at) report_date, 
	trim('"' from (details::json->'message')::text) first_attempt_fail_reason, 
	count(id) first_attempt_failed_attempts
from 
	(select 
		user_id, event_name, level, message, id, created_at, details, 
		lead(level, 1) over(partition by user_id order by id) next_level, 
		row_number() over(partition by user_id order by id asc) seq
	from public.eventapp_event
	where id in 
		(select id
		from systems_monitoring.event_table_temp
		where 
			event_name='check-pin-validity-api'
			and date(created_at)>=current_date-7 and date(created_at)<current_date
		)
	) tbl1 
where 
	seq=2 
	and level='ERROR'
group by 1, 2
order by 1, 2; 

-- 4. #Login failed due to wrong PIN in TallyKhata (unique users count)?
select date(created_at) report_date, fail_reason, count(distinct user_id) users, count(user_id) attempts 
from 
	(select *, trim('"' from (details::json->'message')::text) fail_reason
	from public.eventapp_event
	where id in 
		(select id
		from systems_monitoring.event_table_temp
		where 
			date(created_at)>=current_date-7 and date(created_at)<current_date
			and event_name like '%login%'
			and level='ERROR'
		)
	) tbl1
where fail_reason in 
	('Invalid credentials',
	'You entered wrong PIN 2 times. Your account will be blocked if you enter wrong PIN 1 more time.',
	'Dear valued TallyPay user, for entering wrong PIN 3 times, your account has been blocked for 1 hour. Please try to login later.'
	)
group by 1, 2
order by 1, 2;

-- 3. Any failed case in reset through face verification?
select date(created_at) report_date, count(distinct user_id) face_verify_failed_users, count(id) face_verify_failed_cases
from public.eventapp_event
where id in 
	(select id
	from systems_monitoring.event_table_temp
	where 
		date(created_at)>=current_date-7 and date(created_at)<current_date
		and level='ERROR' 
		and event_name='face-image-verify'
		and message='Error Occurred'
	)
group by 1 
order by 1; 

-- from NP DWH
-- 2. How many users came through the reset path?
select date(created_at) report_date, count(id) reset_requests, count(distinct mobile_no) users_through_reset
from 
	(select *, trim('"' from (long_text::json->'wallet_no')::text) mobile_no
	from nobopay_api_gw.activity_log
	where url in('/api/portal/user/pin/reset', '/api/v1/user/pin/reset')
	) tbl1 
where date(created_at)>=current_date-7 and date(created_at)<current_date
group by 1 
order by 1; 
