/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1446938538
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): investigation of whether participation of new users to DAU declined
*/

-- registered users in the last 30 days
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select mobile_number mobile_no, date(created_at) reg_date
from public.register_usermobile
where date(created_at)>=current_date-30; 

-- last 30 days' registered users' first DAU-date after registration
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select mobile_no, reg_date, min(event_date) first_event_date
from 
	data_vajapora.help_a tbl1 
	inner join 
	tallykhata.tallykhata_user_date_sequence_final tbl2 using(mobile_no)
where event_date>=reg_date
group by 1, 2;

-- distribution of first DAU-dates after registration 
select *
from 
	(select reg_date, count(mobile_no) merchants_registered
	from data_vajapora.help_a
	group by 1
	) tbl1 
	
	inner join 
	
	(select 
		reg_date, 
		count(case when reg_to_first_event_days_cat='first DAU on the day of registration' then mobile_no else null end) "first DAU on the day of registration",
		count(case when reg_to_first_event_days_cat='first DAU within 2 days of registration' then mobile_no else null end) "first DAU within 2 days of registration",
		count(case when reg_to_first_event_days_cat='first DAU within 5 days of registration' then mobile_no else null end) "first DAU within 5 days of registration",
		count(case when reg_to_first_event_days_cat='first DAU within 7 days of registration' then mobile_no else null end) "first DAU within 7 days of registration",
		count(case when reg_to_first_event_days_cat='first DAU after a week of registration' then mobile_no else null end) "first DAU after a week of registration"
	from 
		(select 
			*,
			case 
				when reg_to_first_event_days=0 then 'first DAU on the day of registration'
				when reg_to_first_event_days in(1, 2) then 'first DAU within 2 days of registration'
				when reg_to_first_event_days in(3, 4, 5) then 'first DAU within 5 days of registration'
				when reg_to_first_event_days in(6, 7) then 'first DAU within 7 days of registration'
				when reg_to_first_event_days>7 then 'first DAU after a week of registration'
			end reg_to_first_event_days_cat
		from 
			(select *, first_event_date-reg_date reg_to_first_event_days
			from data_vajapora.help_b
			) tbl1 
		) tbl1 
	group by 1
	) tbl2 using(reg_date)
order by 1; 
