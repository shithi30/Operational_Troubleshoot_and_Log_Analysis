/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1723632967
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

select 
	split_part(os_version, '.', 1) os, 
	count(case when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then mobile_no else null end) "3RAU",
	count(case when tg in('LTUCb','LTUTa') then mobile_no else null end) "LTU",
	count(case when tg in('NB0','NN1','NN2-6') then mobile_no else null end) "NN",
	count(case when tg in('NT--') then mobile_no else null end) "NT",
	count(case when tg in('PSU') then mobile_no else null end) "PSU",
	count(case when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then mobile_no else null end) "PU",
	count(case when tg in('SPU') then mobile_no else null end) "SPU",
	count(case when tg in('ZCb','ZTa','ZTa+Cb') then mobile_no else null end) "Zombie"
from 
	(select mobile_no, tg
	from cjm_segmentation.retained_users
	where report_date=current_date
	) tbl1 
	
	left join 
	
	(select mobile_no, os_version
	from 
		(select id, mobile mobile_no, os_version
		from public.registered_users
		) tbl1 
		
		inner join 
		
		(select mobile mobile_no, max(id) id 
		from public.registered_users 
		group by 1
		) tbl2 using(mobile_no, id)
	) tbl2 using(mobile_no)
group by 1
order by 1;

