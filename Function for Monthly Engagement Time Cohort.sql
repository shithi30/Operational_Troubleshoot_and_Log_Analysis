/*
- Viz: https://docs.google.com/spreadsheets/d/1YAmv4xXw5SbFm5GM0RlD-PctNQBj1JrmJcyu1n7QSuM/edit#gid=738138814
- Data: https://docs.google.com/spreadsheets/d/1YAmv4xXw5SbFm5GM0RlD-PctNQBj1JrmJcyu1n7QSuM/edit#gid=870219352
- Function: data_vajapora.fn_monthly_engagement_time_cohort()
- Table: data_vajapora.monthly_engagement_time_cohort
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_monthly_engagement_time_cohort()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Analysis of retention in terms of monthly average engagement time 
Auxiliary data table(s) : data_vajapora.monthly_engagement_time_cohort_help1, data_vajapora.monthly_engagement_time_cohort_help2
Target data table(s)    : data_vajapora.monthly_engagement_time_cohort
*/

declare
	var_threshold int;

begin
	-- generating monthly threshold for enfagagement minutes, based on last full 3 months' data
	select floor(avg(avg_mins_per_user)) avg_monthly_mins_per_user into var_threshold
	from 
		(select *, row_number() over(order by year_month desc) seq
		from 
			(select 
				to_char(event_date, 'YYYY-MM') year_month, 
				min(event_date) month_start, max(event_date) month_end,
				sum(sec_with_tk)/(count(distinct mobile_no)*60.00) avg_mins_per_user
			from tallykhata.daily_times_spent_individual_data
			where event_date>=current_date-130 -- cover several months past
			group by 1
			) tbl1
		) tbl1
	where seq in(2, 3, 4); -- recent, full 3 months
	raise notice 'Monthly threshold of minutes per user is generated: %', var_threshold; 
	
	-- generating user-wise monthly engagement times
	drop table if exists data_vajapora.monthly_engagement_time_cohort_help1;
	create table data_vajapora.monthly_engagement_time_cohort_help1 as
	select 
		to_char(event_date, 'YYYY-MM') year_month, 
		mobile_no,
		sum(sec_with_tk)/60.00 mins_with_tk
	from tallykhata.daily_times_spent_individual_data
	group by 1, 2
	having sum(sec_with_tk)/60.00>=var_threshold; -- monthly minute-threshold
	raise notice 'Monthly engagement time per user is generated.';
	
	-- generating retention cohort data (numbers)
	drop table if exists data_vajapora.monthly_engagement_time_cohort_help2;
	create table data_vajapora.monthly_engagement_time_cohort_help2 as
	select *, row_number() over(partition by min_year_month order by year_month asc)-1 gap_month
	from 
		(select min_year_month, year_month, count(mobile_no) merchants
		from 
			(select mobile_no, min(year_month) min_year_month
			from data_vajapora.monthly_engagement_time_cohort_help1
			group by 1
			) tbl1 
			
			inner join 
			
			(select mobile_no, year_month
			from data_vajapora.monthly_engagement_time_cohort_help1
			) tbl2 using(mobile_no)
		where min_year_month>='2021-03'
		group by 1, 2
		) tbl1; 
	raise notice 'Retention cohort data (in terms of numbers) are generated.';
	
	-- generating retention cohort data (percentages)
	drop table if exists data_vajapora.monthly_engagement_time_cohort;
	create table data_vajapora.monthly_engagement_time_cohort as
	select min_year_month, year_month, gap_month, merchants, merchants_start, merchants*1.00/merchants_start merchants_from_start_pct
	from 
		data_vajapora.monthly_engagement_time_cohort_help2 tbl1
		
		inner join 
		
		(select min_year_month, merchants merchants_start
		from data_vajapora.monthly_engagement_time_cohort_help2
		where gap_month=0
		) tbl2 using(min_year_month)
	order by 1, 3; 
	raise notice 'Retention cohort data (in terms of percentages) are generated.'; 

	-- dropping auxiliary tables
	drop table if exists data_vajapora.monthly_engagement_time_cohort_help1;
	drop table if exists data_vajapora.monthly_engagement_time_cohort_help2;

END;
$function$
;

/*
select data_vajapora.fn_monthly_engagement_time_cohort();

select *
from data_vajapora.monthly_engagement_time_cohort; 

select *
from data_vajapora.monthly_engagement_time_cohort
where 
	-- discard data of ongoing month
	min_year_month!=to_char(current_date, 'YYYY-MM')
	and year_month!=to_char(current_date, 'YYYY-MM'); 

select gap_month, avg(merchants_from_start_pct) avg_retention_pct
from data_vajapora.monthly_engagement_time_cohort
where 
	-- discard data of ongoing month
	min_year_month!=to_char(current_date, 'YYYY-MM')
	and year_month!=to_char(current_date, 'YYYY-MM')
group by 1
order by 1; 
*/

	