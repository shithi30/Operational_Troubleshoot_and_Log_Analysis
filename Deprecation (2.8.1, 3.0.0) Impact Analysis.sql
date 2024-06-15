/*
- Viz: 
	- Activity: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1495409159
	- Versions of full base: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=683387513
	- Versions of new registrations: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=407351159
	- Conversion to 3.0.1: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=934355091
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

-- deprecation impact on transacting merchants
do $$

declare
	var_date date:='2021-06-10';
begin

	delete from data_vajapora.deprecate_impact_1
	where event_date>=var_date; 

	loop
	
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select tbl1.mobile_no
		from 
			(select mobile_no, max(update_or_reg_datetime) max_date
			from data_vajapora.version_wise_days
			where date(update_or_reg_datetime)<=var_date
			group by 1
			) tbl1
			
			inner join 
			
			(select mobile_no, update_or_reg_datetime
			from data_vajapora.version_wise_days
			where app_version_number in(92, 93)
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.max_date=tbl2.update_or_reg_datetime);
		
		insert into data_vajapora.deprecate_impact_1
		select 
			event_date,
			count(distinct case when entry_type=2 then tbl1.mobile_no else null end) merchants_opened,
			count(distinct case when entry_type=1 then tbl1.mobile_no else null end) merchants_transacted,
			count(distinct case when entry_type=2 and tbl2.mobile_no is not null then tbl1.mobile_no else null end) deprecated_merchants_opened,
			count(distinct case when entry_type=1 and tbl2.mobile_no is not null then tbl1.mobile_no else null end) deprecated_merchants_transacted, 
			case when event_date>='2021-06-28' then 'after deprecation' else 'before deprecation' end deprecation_status
		from 
			(select mobile_no, entry_type, event_date
			from tallykhata.event_transacting_fact 
			where 
				event_date=var_date
				and (event_name='app_opened' or entry_type=1)
			) tbl1 
			
			left join 
				
			data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		group by 1;
		
		raise notice 'Data inserted for: %', var_date; 
	
		var_date=var_date+1;
		if(var_date=current_date) then exit;
		end if; 
	end loop; 

end $$; 

/*
truncate table data_vajapora.deprecate_impact_1; 
select *
from data_vajapora.deprecate_impact_1; 
*/

-- deprecation impact on full merchant-base's versions
do $$

declare
	var_date date:='2021-06-15';
begin

	raise notice 'New OP goes below:'; 
	
	delete from data_vajapora.deprecate_impact_2
	where event_date>=var_date; 

	loop

		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select tbl1.mobile_no, app_version_name, var_date event_date
		from 
			(select mobile_no, max(update_or_reg_datetime) max_datetime
			from data_vajapora.version_wise_days
			where date(update_or_reg_datetime)<=var_date
			group by 1
			) tbl1
			
			inner join 
			
			(select mobile_no, update_or_reg_datetime, app_version_name
			from data_vajapora.version_wise_days
			where app_version_name in('3.0.1', '3.0.0', '2.8.1', '2.8.0', '2.7.1')
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.max_datetime=tbl2.update_or_reg_datetime);
		
		insert into data_vajapora.deprecate_impact_2 
		select 
			event_date,
			count(distinct case when app_version_name='3.0.1' then mobile_no else null end) version_301,
			count(distinct case when app_version_name='3.0.0' then mobile_no else null end) version_300,
			count(distinct case when app_version_name='2.8.1' then mobile_no else null end) version_281,
			count(distinct case when app_version_name='2.8.0' then mobile_no else null end) version_280,
			count(distinct case when app_version_name='2.7.1' then mobile_no else null end) version_271,
			case when event_date>='2021-06-28' then 'after deprecation' else 'before deprecation' end deprecation_status
		from data_vajapora.help_a
		group by 1;

		raise notice 'Data inserted for: %', var_date; 
	
		var_date=var_date+1;
		if(var_date=current_date) then exit;
		end if; 
	end loop; 

end $$; 

/*
truncate table data_vajapora.deprecate_impact_2; 
select *
from data_vajapora.deprecate_impact_2; 
*/

-- daily distribution of versions in newly registered merchants
select 
	date(reg_datetime) reg_date, 
	count(distinct case when app_version_name='3.0.1' then mobile_no else null end) reg_version_301,
	count(distinct case when app_version_name='3.0.0' then mobile_no else null end) reg_version_300,
	count(distinct case when app_version_name='2.8.1' then mobile_no else null end) reg_version_281,
	count(distinct case when app_version_name='2.8.0' then mobile_no else null end) reg_version_280,
	count(distinct case when app_version_name='2.7.1' then mobile_no else null end) reg_version_271,
	case when date(reg_datetime)>='2021-06-28' then 'after deprecation' else 'before deprecation' end deprecation_status
from data_vajapora.version_wise_days
where 
	app_version_name in('3.0.1', '3.0.0', '2.8.1', '2.8.0', '2.7.1')
	and date(reg_datetime)>=current_date-10
group by 1; 

-- users updating to 3.0.1 from different versions
select 
	date(tbl2.update_or_reg_datetime) update_date, 
	
	count(distinct tbl2.mobile_no) merchants_updated_to_301,
	
	count(distinct case when tbl1.app_version_name='3.0.0' then tbl2.mobile_no else null end) merchants_updated_to_301_from_300, 
	count(distinct case when tbl1.app_version_name='2.8.1' then tbl2.mobile_no else null end) merchants_updated_to_301_from_281, 
	count(distinct case when tbl1.app_version_name='2.8.0' then tbl2.mobile_no else null end) merchants_updated_to_301_from_280,
	count(distinct case when tbl1.app_version_name='2.7.1' then tbl2.mobile_no else null end) merchants_updated_to_301_from_271,
	count(distinct case when tbl1.app_version_name not in('3.0.0', '2.8.1', '2.8.0', '2.7.1') then tbl2.mobile_no else null end) merchants_updated_to_301_from_other_versions,
	
	count(distinct case when tbl1.app_version_name='3.0.0' then tbl2.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_updated_to_301_from_300_pct, 
	count(distinct case when tbl1.app_version_name='2.8.1' then tbl2.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_updated_to_301_from_281_pct, 
	count(distinct case when tbl1.app_version_name='2.8.0' then tbl2.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_updated_to_301_from_280_pct,
	count(distinct case when tbl1.app_version_name='2.7.1' then tbl2.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_updated_to_301_from_271_pct,
	count(distinct case when tbl1.app_version_name not in('3.0.0', '2.8.1', '2.8.0', '2.7.1') then tbl2.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_updated_to_301_from_other_versions_pct,                         
	
	case when date(tbl2.update_or_reg_datetime)>='2021-06-28' then 'after' else 'before' end deprecation_status
from 
	(select *, row_number() over(partition by mobile_no order by app_version_number asc) version_seq
	from data_vajapora.version_wise_days
	) tbl1 
	
	inner join 
	
	(select *, row_number() over(partition by mobile_no order by app_version_number asc) version_seq
	from data_vajapora.version_wise_days
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.version_seq=tbl2.version_seq-1)
where 
	tbl2.app_version_name='3.0.1'
	and date(tbl2.update_or_reg_datetime)>='2021-06-15'
group by 1 
order by 1 asc; 
