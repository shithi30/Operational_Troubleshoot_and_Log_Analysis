/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1455953771
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=739010171
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1375561332
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

-- version-01
select 
	report_date, 
	
	count(mobile_no) spus, 
	
	count(case when reg_date>=report_date-21 and reg_date<report_date then mobile_no else null end) reg_in_3_weeks,
	count(case when reg_date>=report_date-42 and reg_date<report_date-21 then mobile_no else null end) reg_in_4_to_6_weeks, 
	count(case when reg_date>=report_date-63 and reg_date<report_date-42 then mobile_no else null end) reg_in_7_to_9_weeks, 
	count(case when reg_date>=report_date-84 and reg_date<report_date-63 then mobile_no else null end) reg_in_10_to_12_weeks, 
	count(case when reg_date>=report_date-105 and reg_date<report_date-84 then mobile_no else null end) reg_in_13_to_15_weeks, 
	count(case when reg_date>=report_date-126 and reg_date<report_date-105 then mobile_no else null end) reg_in_16_to_18_weeks,
	
	count(case when reg_date>=report_date-182 and reg_date<report_date-126 then mobile_no else null end) reg_in_19_to_26_weeks, 
	count(case when reg_date>=report_date-238 and reg_date<report_date-182 then mobile_no else null end) reg_in_27_to_34_weeks, 
	count(case when reg_date>=report_date-294 and reg_date<report_date-238 then mobile_no else null end) reg_in_35_to_42_weeks, 
	count(case when reg_date>=report_date-350 and reg_date<report_date-294 then mobile_no else null end) reg_in_43_to_50_weeks, 
	count(case when reg_date>=report_date-406 and reg_date<report_date-350 then mobile_no else null end) reg_in_51_to_58_weeks,
	
	count(case when reg_date<report_date-406 then mobile_no else null end) reg_in_more_than_58_weeks,
	
	count(case when reg_date>=report_date then mobile_no else null end) reg_after_report_date
from 
	(select report_date, mobile_no
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='SPU'
		and report_date>=current_date-180 and report_date<current_date
	) tbl1 
	
	inner join 
	
	(select date(created_at) reg_date, mobile_number mobile_no
	from public.register_usermobile  
	) tbl2 using(mobile_no)
group by 1 
order by 1; 

-- version-02: exponential of 2 
select 
	report_date, 
	
	count(mobile_no) spus, 
	
	count(case when reg_date>=report_date-14 and reg_date<report_date then mobile_no else null end) reg_in_2_weeks,
	count(case when reg_date>=report_date-28 and reg_date<report_date-14 then mobile_no else null end) reg_in_3_to_4_weeks, 
	count(case when reg_date>=report_date-56 and reg_date<report_date-28 then mobile_no else null end) reg_in_5_to_8_weeks, 
	count(case when reg_date>=report_date-112 and reg_date<report_date-56 then mobile_no else null end) reg_in_9_to_16_weeks, 
	count(case when reg_date>=report_date-224 and reg_date<report_date-112 then mobile_no else null end) reg_in_17_to_32_weeks, 
	count(case when reg_date>=report_date-448 and reg_date<report_date-224 then mobile_no else null end) reg_in_33_to_64_weeks,
	
	count(case when reg_date<report_date-448 then mobile_no else null end) reg_in_more_than_64_weeks,
	
	count(case when reg_date>=report_date then mobile_no else null end) reg_after_report_date
from 
	(select report_date, mobile_no
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='SPU'
		and report_date>=current_date-180 and report_date<current_date
	) tbl1 
	
	inner join 
	
	(select date(created_at) reg_date, mobile_number mobile_no
	from public.register_usermobile  
	) tbl2 using(mobile_no)
group by 1 
order by 1; 

-- version-03: exponential of 3
select 
	report_date, 
	
	count(mobile_no) spus, 
	
	count(case when reg_date>=report_date-21 and reg_date<report_date then mobile_no else null end) reg_in_3_weeks,
	count(case when reg_date>=report_date-63 and reg_date<report_date-21 then mobile_no else null end) reg_in_4_to_9_weeks, 
	count(case when reg_date>=report_date-189 and reg_date<report_date-63 then mobile_no else null end) reg_in_10_to_27_weeks, 
	count(case when reg_date>=report_date-567 and reg_date<report_date-189 then mobile_no else null end) reg_in_28_to_81_weeks, 

	count(case when reg_date<report_date-567 then mobile_no else null end) reg_in_more_than_81_weeks,
	
	count(case when reg_date>=report_date then mobile_no else null end) reg_after_report_date
from 
	(select report_date, mobile_no
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='SPU'
		and report_date>=current_date-180 and report_date<current_date
	) tbl1 
	
	inner join 
	
	(select date(created_at) reg_date, mobile_number mobile_no
	from public.register_usermobile  
	) tbl2 using(mobile_no)
group by 1 
order by 1; 

-- version-03: 3*(2^n)
select 
	report_date, 
	
	count(mobile_no) spus, 
	
	count(case when reg_date>=report_date-21 and reg_date<report_date then mobile_no else null end) reg_in_3_weeks,
	count(case when reg_date>=report_date-42 and reg_date<report_date-21 then mobile_no else null end) reg_in_4_to_6_weeks, 
	count(case when reg_date>=report_date-84 and reg_date<report_date-42 then mobile_no else null end) reg_in_7_to_12_weeks, 
	count(case when reg_date>=report_date-168 and reg_date<report_date-84 then mobile_no else null end) reg_in_13_to_24_weeks, 
	count(case when reg_date>=report_date-336 and reg_date<report_date-168 then mobile_no else null end) reg_in_25_to_48_weeks, 
	count(case when reg_date>=report_date-672 and reg_date<report_date-336 then mobile_no else null end) reg_in_49_to_96_weeks, 
	
	count(case when reg_date>=report_date then mobile_no else null end) reg_after_report_date
from 
	(select report_date, mobile_no
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='SPU'
		and report_date>=current_date-180 and report_date<current_date
	) tbl1 
	
	inner join 
	
	(select date(created_at) reg_date, mobile_number mobile_no
	from public.register_usermobile  
	) tbl2 using(mobile_no)
group by 1 
order by 1; 

-- version-04: 3*(2^n) improved
select 
	report_date, 
	
	count(mobile_no) spus, 
	
	count(case when reg_date>=report_date-42 and reg_date<report_date then mobile_no else null end) reg_in_6_weeks, 
	count(case when reg_date>=report_date-84 and reg_date<report_date-42 then mobile_no else null end) reg_in_7_to_12_weeks, 
	count(case when reg_date>=report_date-168 and reg_date<report_date-84 then mobile_no else null end) reg_in_13_to_24_weeks, 
	count(case when reg_date>=report_date-336 and reg_date<report_date-168 then mobile_no else null end) reg_in_25_to_48_weeks, 
	count(case when reg_date>=report_date-672 and reg_date<report_date-336 then mobile_no else null end) reg_in_49_to_96_weeks, 
	
	count(case when reg_date<report_date-672 then mobile_no else null end) reg_in_more_than_96_weeks, 
	
	count(case when reg_date>=report_date then mobile_no else null end) reg_after_report_date
from 
	(select report_date, mobile_no
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='SPU'
		and report_date>=current_date-180 and report_date<current_date
	) tbl1 
	
	inner join 
	
	(select date(created_at) reg_date, mobile_number mobile_no
	from public.register_usermobile  
	) tbl2 using(mobile_no)
group by 1 
order by 1; 
