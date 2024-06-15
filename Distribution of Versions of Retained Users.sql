/*
- Viz: https://docs.google.com/spreadsheets/d/1dc2D-Xl0jzs2EuF6gr7F-WgWX_VVbSjHC8YE4pm6qPw/edit#gid=1349875277
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
*/

select app_version, merchants_in_version, merchants_in_version*1.00/retained_merchants merchants_in_version_pct
from 
	(select 
		case 
			when app_version like '2%' then '2.X.X'
			when app_version like '1%' then '1.X.X'
			when app_version like '0%' then '0.X.X'
			else app_version 
		end app_version,
		count(mobile_no) merchants_in_version
	from cjm_segmentation.retained_users
	where report_date=current_date 
	group by 1
	) tbl1, 
	
	(select count(mobile_no) retained_merchants
	from cjm_segmentation.retained_users
	where report_date=current_date
	) tbl2
order by 1; 



