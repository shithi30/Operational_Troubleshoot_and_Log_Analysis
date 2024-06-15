/*
- Viz: https://docs.google.com/spreadsheets/d/1P3AzrkqZuDzaJmDNwj6h-flSUi7RgyqF4FWVL7Uc2tI/edit#gid=1273663233
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
*/

-- first DAU TACS tendency 
select *
from 
	(-- for 3.0.2
	select 
		reg_hour_of_day, 
		count(distinct tbl2.mobile_no)*1.00/count(distinct tbl1.mobile_no) merchants_added_tacs_pct_302
	from 
		(select mobile_no, date_part('hour', reg_datetime) reg_hour_of_day
		from data_vajapora.version_wise_days
		where 
			left(update_or_reg_datetime::text, 19)=left(reg_datetime::text, 19)
			and app_version_name='3.0.2'
			and date(reg_datetime)='2021-07-15'
		) tbl1 
		
		left join 
		
		(select mobile_no, contact, create_date::timestamp 
		from public.account 
		where 
			date(create_date)='2021-07-15'
		    and type in(2, 3)
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no) 
	group by 1
	) tbl1 
	
	inner join 
	
	(-- for 3.0.1
	select 
		reg_hour_of_day, 
		count(distinct tbl2.mobile_no)*1.00/count(distinct tbl1.mobile_no) merchants_added_tacs_pct_301 
	from 
		(select mobile_no, date_part('hour', reg_datetime) reg_hour_of_day
		from data_vajapora.version_wise_days
		where 
			left(update_or_reg_datetime::text, 19)=left(reg_datetime::text, 19)
			and app_version_name='3.0.1'
			and date(reg_datetime)='2021-07-08'
		) tbl1 
		
		left join 
		
		(select mobile_no, contact, create_date::timestamp 
		from public.account 
		where 
			date(create_date)='2021-07-08'
		    and type in(2, 3)
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no) 
	group by 1
	) tbl2 using(reg_hour_of_day); 

-- first DAU TRT tendency 
select *
from 
	(-- for 3.0.2
	select 
		reg_hour_of_day, 
		count(distinct tbl2.mobile_no)*1.00/count(distinct tbl1.mobile_no) merchants_added_trt_pct_302
	from 
		(select mobile_no, date_part('hour', reg_datetime) reg_hour_of_day
		from data_vajapora.version_wise_days
		where 
			left(update_or_reg_datetime::text, 19)=left(reg_datetime::text, 19)
			and app_version_name='3.0.2'
			and date(reg_datetime)='2021-07-15'
		) tbl1 
		
		left join 
		
		(select mobile_no
		from public.journal 
		where date(create_date)='2021-07-15'
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no) 
	group by 1
	) tbl1 
	
	inner join 
	
	(-- for 3.0.1
	select 
		reg_hour_of_day, 
		count(distinct tbl2.mobile_no)*1.00/count(distinct tbl1.mobile_no) merchants_added_trt_pct_301 
	from 
		(select mobile_no, date_part('hour', reg_datetime) reg_hour_of_day
		from data_vajapora.version_wise_days
		where 
			left(update_or_reg_datetime::text, 19)=left(reg_datetime::text, 19)
			and app_version_name='3.0.1'
			and date(reg_datetime)='2021-07-08'
		) tbl1 
		
		left join 
		
		(select mobile_no
		from public.journal 
		where date(create_date)='2021-07-08'
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no) 
	group by 1
	) tbl2 using(reg_hour_of_day); 
