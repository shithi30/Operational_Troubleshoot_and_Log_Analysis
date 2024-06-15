/*
- Viz: https://docs.google.com/spreadsheets/d/1P3AzrkqZuDzaJmDNwj6h-flSUi7RgyqF4FWVL7Uc2tI/edit#gid=1275491838
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
	count(tbl2.mobile_no) merchants_updated
from 
	(select *, row_number() over(partition by mobile_no order by update_or_reg_datetime asc) version_seq
	from data_vajapora.version_wise_days
	) tbl1 
	
	inner join 
	
	(select *, row_number() over(partition by mobile_no order by update_or_reg_datetime asc) version_seq
	from data_vajapora.version_wise_days
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.version_seq=tbl2.version_seq-1)
where 
	tbl2.app_version_name in('3.0.2')
	and date(tbl2.update_or_reg_datetime)>='2021-07-14'
group by 1
having count(tbl2.mobile_no)>1 
order by 1; 