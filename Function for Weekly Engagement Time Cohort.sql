/*
- Viz: https://docs.google.com/spreadsheets/d/1YAmv4xXw5SbFm5GM0RlD-PctNQBj1JrmJcyu1n7QSuM/edit#gid=675115409
- Data: https://docs.google.com/spreadsheets/d/1YAmv4xXw5SbFm5GM0RlD-PctNQBj1JrmJcyu1n7QSuM/edit#gid=0
- Function: data_vajapora.fn_weekly_engagement_time_cohort()
- Table: data_vajapora.weekly_engagement_time_cohort
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_weekly_engagement_time_cohort()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Analysis of retention in terms of weekly average engagement time 
Auxiliary data table(s) : data_vajapora.weekly_engagement_time_cohort_help1, data_vajapora.weekly_engagement_time_cohort_help2
Target data table(s)    : data_vajapora.weekly_engagement_time_cohort
*/

declare
	var_threshold int;

begin
	-- generating weekly engagement time-threshold, from recent full weeks' tendencies
	select floor(avg(avg_mins_per_user)) avg_weekly_mins_per_user into var_threshold
	from 
		(select 
			to_char(event_date, 'YYYY-WW') year_week, 
			min(event_date) week_start, max(event_date) week_end,
			sum(sec_with_tk)/(count(distinct mobile_no)*60.00) avg_mins_per_user 
		from tallykhata.daily_times_spent_individual_data
		where event_date>=current_date-60 -- recent
		group by 1
		having max(event_date)-min(event_date)=6 -- full weeks
		) tbl1;
	raise notice 'Weekly threshold of minutes per user is generated: %', var_threshold; 
	
	-- generating data of weekly users who spent >=threshold mins, with week terminals
	drop table if exists data_vajapora.weekly_engagement_time_cohort_help1;
	create table data_vajapora.weekly_engagement_time_cohort_help1 as
	select *
	from 
		(select 
			to_char(event_date, 'YYYY-WW') year_week, 
			mobile_no,
			sum(sec_with_tk)/60.00 mins_with_tk
		from tallykhata.daily_times_spent_individual_data
		group by 1, 2
		having sum(sec_with_tk)/60.00>=21
		) tbl1
		
		inner join 
		
		(select 
			to_char(event_date, 'YYYY-WW') year_week, 
			min(event_date) week_start, max(event_date) week_end
		from tallykhata.daily_times_spent_individual_data
		group by 1
		having max(event_date)-min(event_date)=6
		) tbl2 using(year_week); 
	raise notice 'Weekly engagement time per user is generated.';
	
	-- generating main data for retention cohort
	drop table if exists data_vajapora.weekly_engagement_time_cohort_help2;
	create table data_vajapora.weekly_engagement_time_cohort_help2 as
	select min_year_week, min_week_start, min_week_end, gap_week, count(mobile_no) merchants
	from 
		(select *, (week_end-min_week_end)/7.00 gap_week -- for calculating gaps in weeks
		from 
			(select mobile_no, min(year_week) min_year_week, min(week_start) min_week_start, min(week_end) min_week_end
			from data_vajapora.weekly_engagement_time_cohort_help1
			group by 1
			) tbl1 
			
			inner join 
			
			(select mobile_no, year_week, week_end
			from data_vajapora.weekly_engagement_time_cohort_help1
			) tbl2 using(mobile_no)
		order by 1, 5
		) tbl1
	group by 1, 2, 3, 4
	order by 1, 4; 
	raise notice 'Retention cohort data (in terms of numbers) are generated.';
		
	-- generating cohort data in terms of percentage
	drop table if exists data_vajapora.weekly_engagement_time_cohort;
	create table data_vajapora.weekly_engagement_time_cohort as
	select min_year_week, concat(min_week_start, ' to ', min_week_end) week_start_end, gap_week, merchants, merchants_start, merchants*1.00/merchants_start merchants_from_start_pct
	from 
		data_vajapora.weekly_engagement_time_cohort_help2 tbl1
		
		inner join 
		
		(select min_year_week, merchants merchants_start
		from data_vajapora.weekly_engagement_time_cohort_help2
		where gap_week=0
		) tbl2 using(min_year_week)
	where min_week_end>=current_date-90 -- recent
	order by 1, 3; 
	raise notice 'Retention cohort data (in terms of percentages) are generated.'; 
	
	-- dropping auxiliary tables
	drop table if exists data_vajapora.weekly_engagement_time_cohort_help1;
	drop table if exists data_vajapora.weekly_engagement_time_cohort_help2;

END;
$function$
;

/*
select data_vajapora.fn_weekly_engagement_time_cohort();

select *
from data_vajapora.weekly_engagement_time_cohort; 

select gap_week, avg(merchants_from_start_pct) avg_retention_pct
from data_vajapora.weekly_engagement_time_cohort
group by 1
order by 1; 
*/
