/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1130400231
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 

Churn users analysis: (Users who did nothing in last 14 days can be considered as churn user)
--> Classification of churn users 
--> 1st day churn/ 1st 2 days churn/1st week churn/1st 15 days churn/1st 30 days churn/After >30 days churn ( from registration date)
--> find the activity of last 1 hour before they churn.
--> draw a heat-map.

*/

drop table if exists data_vajapora.user_date_seq;
create table data_vajapora.user_date_seq as
select *, row_number() over(partition by mobile_no order by created_datetime) user_date_seq
from 
	(select distinct mobile_no, created_datetime
	from tallykhata.tallykhata_fact_info_final
	) tbl1; 

select 
	reg_date, 
	
	count(mobile_no) reg_to_churn, 
	
	count(case when churn_within_days_excl_cat='churn on reg day' then mobile_no else null end) "churn on reg day",
	count(case when churn_within_days_excl_cat='churn within 1 to 2 days' then mobile_no else null end) "churn within 1 to 2 days",
	count(case when churn_within_days_excl_cat='churn within 3 to 7 days' then mobile_no else null end) "churn within 3 to 7 days",
	count(case when churn_within_days_excl_cat='churn within 8 to 15 days' then mobile_no else null end) "churn within 8 to 15 days",
	count(case when churn_within_days_excl_cat='churn within 16 to 30 days' then mobile_no else null end) "churn within 16 to 30 days",
	count(case when churn_within_days_excl_cat='churn within >30 days' then mobile_no else null end) "churn within >30 days",
	
	count(case when churn_within_days<=2 then mobile_no else null end) "churn within 2 days",
	count(case when churn_within_days<=7 then mobile_no else null end) "churn within 7 days",
	count(case when churn_within_days<=15 then mobile_no else null end) "churn within 15 days",
	count(case when churn_within_days<=30 then mobile_no else null end) "churn within 30 days"
from 
	(select 
		*, 
		last_act_date-reg_date churn_within_days,
		case 
			when (last_act_date-reg_date)=0 then 'churn on reg day'
			when (last_act_date-reg_date)>=1 and (last_act_date-reg_date)<=2 then 'churn within 1 to 2 days'
			when (last_act_date-reg_date)>=3 and (last_act_date-reg_date)<=7 then 'churn within 3 to 7 days'
			when (last_act_date-reg_date)>=8 and (last_act_date-reg_date)<=15 then 'churn within 8 to 15 days'
			when (last_act_date-reg_date)>=16 and (last_act_date-reg_date)<=30 then 'churn within 16 to 30 days'
			else 'churn within >30 days'
		end churn_within_days_excl_cat
	from 
		(select mobile_no, max(created_datetime) last_act_date
		from data_vajapora.user_date_seq 
		group by 1 
		having current_date-max(created_datetime)>=14
		) tbl1 
		
		inner join 
		
		(select mobile_number mobile_no, date(created_at) reg_date 
		from public.register_usermobile 
		) tbl2 using(mobile_no)
	where 
		last_act_date>=reg_date and last_act_date<=current_date
		and reg_date>='2020-07-01' and reg_date<=current_date-45
	) tbl1
group by 1 
order by 1 asc; 

