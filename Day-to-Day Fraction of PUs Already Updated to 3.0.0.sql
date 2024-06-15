/*
- Viz: 310.png
- Data: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit#gid=1410924943
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

select 
	report_date,
	count(distinct mobile_no) pus,
	count(distinct updated_mobile_no) updated_pus,
	count(distinct updated_mobile_no)*1.00/count(distinct mobile_no) updated_pus_pct
from 
	(select mobile_no, report_date 
	from tallykhata.tallykhata_usages_data_temp_v1
	where
		report_date>='2021-04-15'
		and total_active_days>=10
	) tbl1
	
	left join 
	
	(select mobile_no updated_mobile_no
	from data_vajapora.version_wise_days
	where 
		app_version_name='3.0.0'
		and date(update_or_reg_datetime)>date(reg_datetime)
	) tbl2 on(tbl1.mobile_no=tbl2.updated_mobile_no)
group by 1 
order by 1; 
