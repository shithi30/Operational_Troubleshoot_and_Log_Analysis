/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=429025485
- Notes: 
	User who uninstalled the app pls share their transaction distribution.
	like total 10 lac user uninstalled the app out of them - 
	--> Only did 1 activity - 5 lac
	--> Only did 2 activity - 3 lac
	--> Did >2 activity 2 lac
	--> Did >10 activity 50 k
	--> Did > 50 activity 25k 
	--> Did >100 activity 10k
	@Shithi pls share a report like this.
	Look test.inactive_numbers for uninstalled base. 
*/

-- in terms of TRT
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select mobile_no, count(auto_id) trt
from 
	test.inactive_numbers tbl1
	left join 
	tallykhata.tallykhata_fact_info_final tbl2 using(mobile_no)
group by 1; 

select 
	count(*) all_uninstalled_merchants,
	count(case when trt=0 then mobile_no else null end) trt_0,
	count(case when trt>=1 and trt<=5 then mobile_no else null end) trt_1_to_5,
	count(case when trt>=6 and trt<=10 then mobile_no else null end) trt_6_to_10,
	count(case when trt>=11 and trt<=15 then mobile_no else null end) trt_11_to_15,
	count(case when trt>=16 and trt<=30 then mobile_no else null end) trt_16_to_30,
	count(case when trt>=31 and trt<=50 then mobile_no else null end) trt_31_to_50,
	count(case when trt>50 then mobile_no else null end) trt_more_than_50
from data_vajapora.help_a; 

-- in terms of time spent
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select mobile_no, sum(sec_with_tk)/60.00 min_with_tk
from 
	test.inactive_numbers tbl1
	left join 
	tallykhata.daily_times_spent_individual_data tbl2 using(mobile_no)
group by 1; 

select
	count(*) all_uninstalled_merchants,
	count(case when min_with_tk=0 or min_with_tk is null then mobile_no else null end) no_min_found,
	count(case when min_with_tk>0 and min_with_tk<5 then mobile_no else null end) min_1_to_5,
	count(case when min_with_tk>=5 and min_with_tk<10 then mobile_no else null end) min_5_to_10,
	count(case when min_with_tk>=10 and min_with_tk<15 then mobile_no else null end) min_10_to_15,
	count(case when min_with_tk>=15 and min_with_tk<30 then mobile_no else null end) min_15_to_30,
	count(case when min_with_tk>=30 and min_with_tk<60 then mobile_no else null end) min_30_to_60,
	count(case when min_with_tk>=60 then mobile_no else null end) min_more_than_60
from data_vajapora.help_b;

