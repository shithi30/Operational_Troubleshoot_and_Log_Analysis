/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=509951737
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 

I tried to analyze, if users who took version-updates previously, are more prone to updating.

For this, I took 3.0.0 updaters who had been in 3RAU at least once. Findings:
- By day-4 of launching, users who took previous updates showed ~6% greater tendency of updating.  

Previous experience of having data intact after updating may have been an encouraging factor for updating, which was absent for the other group. But this gap gradually closes as days progress. 

*/

drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select days_to_update, count(mobile_no) updated_merchants
from 
	(select mobile_no, date(update_or_reg_datetime)-'2021-05-09'::date days_to_update
	from data_vajapora.version_wise_days
	where 
		app_version_name='3.0.0'
		and date(update_or_reg_datetime)-'2021-05-09'::date>=0
	) tbl1 
	
	inner join 
	
	(select mobile_no, case when date(update_or_reg_datetime)!=date(reg_datetime) then 1 else 0 end if_updated_previously
	from data_vajapora.version_wise_days
	where app_version_name='2.8.1'
	) tbl2 using(mobile_no)
	
	inner join 
	
	(select distinct mobile_no
	from tallykhata.tallykhata_regular_active_user 
	where rau_category=3
	) tbl3 using(mobile_no)
where if_updated_previously=1 -- change to: 0/1
group by 1; 

select 
	tbl1.days_to_update, 
	tbl1.updated_merchants, 
	tbl3.total_updated_merchants, 
	sum(tbl2.updated_merchants) cum_updated_merchants,
	sum(tbl2.updated_merchants)*1.00/tbl3.total_updated_merchants cum_updated_merchants_pct
from 
	data_vajapora.help_a tbl1
	inner join 
	data_vajapora.help_a tbl2 on(tbl1.days_to_update>=tbl2.days_to_update),
	
	(select sum(updated_merchants) total_updated_merchants 
	from data_vajapora.help_a
	) tbl3
group by 1, 2, 3
order by 1 asc; 
	