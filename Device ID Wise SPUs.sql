/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=160990443
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

/*
-- bring from live: data_vajapora.fcm_help_a
select mobile_no, device_id
from 
    (select id, device_id, mobile mobile_no, created_at device_created_at
    from public.registered_users 
    ) tbl1

    inner join 

    (select mobile mobile_no, max(id) id
    from public.registered_users 
    where device_status='active'
    group by 1
    ) tbl2 using(id, mobile_no); 
*/

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select * 
from 
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where pu_type in('SPU', 'Sticky SPU')
	) tbl1 
	
	left join 
	
	data_vajapora.fcm_help_a tbl2 using(mobile_no); 

select *, spus_mobile_no-spus_device_id spus_having_multiple_numbers
from   
	(select 
		report_date, 
		count(distinct mobile_no) spus_mobile_no, 
		count(distinct device_id) spus_device_id
	from data_vajapora.help_a
	group by 1
	order by 1
	) tbl1;
