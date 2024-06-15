/*
-- verify that public.register_historicalregistereduser is ETLed per hour
select distinct left(updated_at::varchar, 13) tbl_update_hr
from public.register_historicalregistereduser 
where date(updated_at)=current_date
order by 1 asc; 
*/

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *, row_number() over(partition by mobile_no order by update_datetime asc, app_version_name asc) version_seq
from 
	(select mobile mobile_no, app_version_name, min(history_date) update_datetime
	from public.register_historicalregistereduser 
	group by 1, 2
	) tbl1;

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select
	tbl1.mobile_no, 
	tbl1.app_version_name,
	tbl1.update_datetime update_or_reg_datetime,
	tbl0.reg_datetime, 
	case 
		when tbl2.update_datetime is not null then date(tbl2.update_datetime)-date(tbl1.update_datetime)
		else current_date-date(tbl1.update_datetime)
	end days_in_version
from 
	(select mobile_number mobile_no, created_at reg_datetime
	from public.register_usermobile
	) tbl0
	inner join
	data_vajapora.help_a tbl1 using(mobile_no)
	left join 
	data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.version_seq=tbl2.version_seq-1)
order by 1 asc, 3 asc, 2 asc;
select *
from data_vajapora.help_b;

/*
-- cases where registrations were done with a specified version 
select *
from data_vajapora.help_b
where 
	left(update_or_reg_datetime::varchar, 19)=left(reg_datetime::varchar, 19)
	and app_version_name='2.8.1';
*/

/*
-- versions on a specific date
select mobile_no, max(app_version_name) current_version
from data_vajapora.help_b 
where date(update_or_reg_datetime)<=current_date 
group by 1 	
*/
