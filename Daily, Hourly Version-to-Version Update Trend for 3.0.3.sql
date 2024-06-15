/*
- Viz: 
	- hourly: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=90289017
	- daily: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1147608115
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
*/

-- hourly update trend
select 
	case 
		when date_part('hour', tbl2.update_or_reg_datetime)<10 then concat(date(tbl2.update_or_reg_datetime), ' Hour: 0', date_part('hour', tbl2.update_or_reg_datetime))
		else concat(date(tbl2.update_or_reg_datetime), ' Hour: ', date_part('hour', tbl2.update_or_reg_datetime)) 
	end date_and_hour,
	
	count(tbl2.mobile_no) merchants_updated,
	
	count(case when tbl1.app_version_name='3.0.2' then tbl2.mobile_no else null end) updated_from_302,
	count(case when tbl1.app_version_name='3.0.1' then tbl2.mobile_no else null end) updated_from_301,
	count(case when tbl1.app_version_name='3.0.0' then tbl2.mobile_no else null end) updated_from_300,
	count(case when tbl1.app_version_name='2.8.1' then tbl2.mobile_no else null end) updated_from_281,
	count(case when tbl1.app_version_name='2.7.1' then tbl2.mobile_no else null end) updated_from_271,
	count(case when tbl1.app_version_name='2.6.1' then tbl2.mobile_no else null end) updated_from_261,
	count(case when tbl1.app_version_name not in('2.6.1', '2.7.1', '2.8.1', '3.0.0', '3.0.1', '3.0.2') then tbl2.mobile_no else null end) updated_from_lower_versions
from 
	(select *, row_number() over(partition by mobile_no order by update_or_reg_datetime asc) version_seq
	from data_vajapora.version_wise_days
	) tbl1 
	
	inner join 
	
	(select *, row_number() over(partition by mobile_no order by update_or_reg_datetime asc) version_seq
	from data_vajapora.version_wise_days
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.version_seq=tbl2.version_seq-1)
where 
	tbl2.app_version_name in('3.0.3')
	and date(tbl2.update_or_reg_datetime)>='2021-07-17'
group by 1
order by 1; 

-- daily update trend
select 
	date(tbl2.update_or_reg_datetime) update_date,
	
	count(tbl2.mobile_no) merchants_updated,
	
	count(case when tbl1.app_version_name='3.0.2' then tbl2.mobile_no else null end) updated_from_302,
	count(case when tbl1.app_version_name='3.0.1' then tbl2.mobile_no else null end) updated_from_301,
	count(case when tbl1.app_version_name='3.0.0' then tbl2.mobile_no else null end) updated_from_300,
	count(case when tbl1.app_version_name='2.8.1' then tbl2.mobile_no else null end) updated_from_281,
	count(case when tbl1.app_version_name='2.7.1' then tbl2.mobile_no else null end) updated_from_271,
	count(case when tbl1.app_version_name='2.6.1' then tbl2.mobile_no else null end) updated_from_261,
	count(case when tbl1.app_version_name not in('2.6.1', '2.7.1', '2.8.1', '3.0.0', '3.0.1', '3.0.2') then tbl2.mobile_no else null end) updated_from_lower_versions
from 
	(select *, row_number() over(partition by mobile_no order by update_or_reg_datetime asc) version_seq
	from data_vajapora.version_wise_days
	) tbl1 
	
	inner join 
	
	(select *, row_number() over(partition by mobile_no order by update_or_reg_datetime asc) version_seq
	from data_vajapora.version_wise_days
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.version_seq=tbl2.version_seq-1)
where 
	tbl2.app_version_name in('3.0.3')
	and date(tbl2.update_or_reg_datetime)>='2021-07-17'
group by 1
order by 1; 