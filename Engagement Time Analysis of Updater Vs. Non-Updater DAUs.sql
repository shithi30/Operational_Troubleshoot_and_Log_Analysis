/*
- Viz: 309.png
- Data: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit#gid=14186381
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 

I analyzed DAUs' engagement time since 2021-05-09. 
DAUs who updated, spent significantly greater minutes on the day of their update. 

Data of 2021-05-11 is expected to show similar patterns after getting fully synced. 

*/

select 
	event_date, 
	count(mobile_no) all_daus,
	count(case when updated_mobile_no is not null then mobile_no else null end) updater_daus,
	count(case when updated_mobile_no is null then mobile_no else null end) non_updater_daus,
	avg(case when updated_mobile_no is not null then sec_with_tk else null end)/60.00 avg_mins_updater_daus,
	avg(case when updated_mobile_no is null then sec_with_tk else null end)/60.00 avg_mins_non_updater_daus
from 
	(select distinct mobile_no, event_date
	from tallykhata.tallykhata_sync_event_fact_final
	where event_date>='2021-05-09' and event_date<current_date
	) tbl1 
	
	left join 
	
	(select mobile_no updated_mobile_no, date(update_or_reg_datetime) update_date
	from data_vajapora.version_wise_days
	where 
		app_version_name='3.0.0'
		and date(update_or_reg_datetime)>date(reg_datetime)
	) tbl2 on(tbl1.mobile_no=tbl2.updated_mobile_no and tbl1.event_date=tbl2.update_date)
	
	left join 
		
	(select mobile_no sec_available_mobile, sec_with_tk, event_date sec_available_date
	from tallykhata.daily_times_spent_individual
	where event_date>='2021-05-09'
	) tbl3 on(tbl1.mobile_no=tbl3.sec_available_mobile and tbl1.event_date=tbl3.sec_available_date)
group by 1
order by 1 asc; 
