/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1517271708
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1493552311
- Email thread: 
- Notes (if any): Run separately and paste on sheet. 
*/

select 
	date, 
	count(tbl1.mobile_no) daus, 
	count(tbl2.mobile_no) daus_updated_to_303,
	count(tbl2.mobile_no)*1.00/count(tbl1.mobile_no) daus_updated_to_303_pct
from 
	(select event_date date, mobile_no 
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date>=current_date-30 and event_date<current_date
	) tbl1 
	
	left join 
	
	(select mobile_no, latest_version
	from tallykhata.tk_user_app_version
	where latest_version='3.0.3'
	) tbl2 using(mobile_no)
group by 1
order by 1; 

select 
	date, 
	count(tbl1.mobile_no) pus, 
	count(tbl2.mobile_no) pus_updated_to_303,
	count(tbl2.mobile_no)*1.00/count(tbl1.mobile_no) pus_updated_to_303_pct
from 
	(select distinct report_date date, mobile_no 
	from tallykhata.tk_power_users_10 
	where report_date>=current_date-30 and report_date<current_date
	) tbl1 
	
	left join 
	
	(select mobile_no, latest_version
	from tallykhata.tk_user_app_version
	where latest_version='3.0.3'
	) tbl2 using(mobile_no)
group by 1
order by 1; 

select 
	date, 
	count(tbl1.mobile_no) raus, 
	count(tbl2.mobile_no) raus_updated_to_303,
	count(tbl2.mobile_no)*1.00/count(tbl1.mobile_no) raus_updated_to_303_pct
from 
	(select rau_date date, mobile_no
	from tallykhata.tallykhata_regular_active_user 
	where 
		rau_category=3
		and rau_date>=current_date-30 and rau_date<current_date
	) tbl1 
	
	left join 
	
	(select mobile_no, latest_version
	from tallykhata.tk_user_app_version
	where latest_version='3.0.3'
	) tbl2 using(mobile_no)
group by 1
order by 1; 
