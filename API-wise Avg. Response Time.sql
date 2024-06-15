/*
- Viz: 
- Data: 
	version-01: 
	- https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=864639925
	- https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=244499549
	version-02:
	- https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=1245151188
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

-- version-01
select to_char(created_at, 'YYYY-MM-DD hour: HH24') date_hour, event_name, avg(split_part(split_part(message, 'response generated in ', 2), ' seconds', 1)::numeric) avg_response_time
from tallykhata.eventapp_event_temp 
where message like 'response generated in%'
group by 1, 2 
order by 1; 

select event_name, avg(split_part(split_part(message, 'response generated in ', 2), ' seconds', 1)::numeric) avg_response_time
from tallykhata.eventapp_event_temp 
where message like 'response generated in%'
group by 1; 

select event_name, count(id) delayed_responses, count(distinct user_id) effected_users 
from 
	(select id, user_id, event_name, split_part(split_part(message, 'response generated in ', 2), ' seconds', 1)::numeric response_time
	from tallykhata.eventapp_event_temp 
	where message like 'response generated in%'
	) tbl1 
where response_time>=3
group by 1 
order by 2 desc; 

select 
	event_name, 
	concat('response in >=', case when round(response_time/3)*3>15 then -1 else round(response_time/3)*3 end, ' seconds') response_slot_in_3s, 
	count(id) delayed_cases 
from 
	(select id, user_id, event_name, split_part(split_part(message, 'response generated in ', 2), ' seconds', 1)::numeric response_time
	from tallykhata.eventapp_event_temp 
	where message like 'response generated in%'
	) tbl1 
where response_time>=3
group by 1, 2
order by 1, 2

-- version-02
select 
	date(created_at) event_date, 
	
	event_name, 
	
	count(id) responses, 
	
	count(case when total_time>0.0 and total_time<=0.1 then id else null end) "d2s_responses_0.1s", 
	count(case when total_time>0.1 and total_time<=0.2 then id else null end) "d2s_responses_0.2s", 
	count(case when total_time>0.2 and total_time<=0.3 then id else null end) "d2s_responses_0.3s", 
	count(case when total_time>0.3 and total_time<=0.4 then id else null end) "d2s_responses_0.4s", 
	count(case when total_time>0.4 and total_time<=0.5 then id else null end) "d2s_responses_0.5s", 
	
	count(case when total_time>0.5 and total_time<=1.0 then id else null end) "d2s_responses_1.0s", 
	count(case when total_time>1.0 and total_time<=2.0 then id else null end) "d2s_responses_2.0s", 
	count(case when total_time>2.0 and total_time<=3.0 then id else null end) "d2s_responses_3.0s", 
	
	count(case when total_time>3.0 then id else null end) "d2s_responses_>3.0s"
from test.eventapp_event_last_7days
group by 1, 2 
order by 1;