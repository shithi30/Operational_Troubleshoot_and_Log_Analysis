/*
- Viz: 
- Data: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=2082501881
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=2129684812
- Function: 
- Table:
- Instructions: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit?pli=1#gid=1529678974
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

/*
-- su_churn_7_days_open
create table data_vajapora.su_churn_7_days_open_summary(report_date date, su_churned_in_7_day_frame int); 
create table data_vajapora.su_churn_7_days_open_details(report_date date, mobile_no text); 
*/

do $$ 

declare 
	var_date date:='2022-03-01'::date; 
begin 
	raise notice 'New OP goes below:'; 	

	loop
		delete from data_vajapora.su_churn_7_days_open_summary
		where report_date=var_date;
		delete from data_vajapora.su_churn_7_days_open_details
		where report_date=var_date;
	
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select * 
		from 
			(select mobile_no su_mobile_no
			from tallykhata.tk_spu_aspu_data 
			where 
				pu_type in('SPU')
				and pu_subtype is null
				and report_date=var_date-1
			) tbl1 
			
			left join 
			
			(select distinct mobile_no active_mobile_no
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date>=var_date and event_date<var_date+7
			) tbl2 on(tbl1.su_mobile_no=tbl2.active_mobile_no);
	
		insert into data_vajapora.su_churn_7_days_open_summary
		select 
			var_date report_date, 
			count(case when active_mobile_no is null then su_mobile_no else null end) su_churned_in_7_day_frame 
		from 
			data_vajapora.help_a tbl1
			
			left join 
			
			(select mobile_no 
			from data_vajapora.su_churn_7_days_open_details 
			where report_date>=var_date-7 and report_date<var_date
			) tbl2 on(tbl1.su_mobile_no=tbl2.mobile_no)
		where tbl2.mobile_no is null;
			
		insert into data_vajapora.su_churn_7_days_open_details 
		select tbl1.* 
		from 
			(select var_date report_date, su_mobile_no mobile_no
			from data_vajapora.help_a
			where active_mobile_no is null
			) tbl1 
			
			left join 
			
			(select mobile_no 
			from data_vajapora.su_churn_7_days_open_details 
			where report_date>=var_date-7 and report_date<var_date
			) tbl2 using(mobile_no)
		where tbl2.mobile_no is null; 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date-7 then exit; 
		end if;
	end loop;
end $$; 

select *
from data_vajapora.su_churn_7_days_open_summary 
order by 1; 

select report_date, count(mobile_no) churned_spus
from data_vajapora.su_churn_7_days_open_details
group by 1
order by 1; 

/*
-- su_churn_7_days_txn
create table data_vajapora.su_churn_7_days_txn_summary(report_date date, su_churned_in_7_day_frame int); 
create table data_vajapora.su_churn_7_days_txn_details(report_date date, mobile_no text); 
*/

do $$ 

declare 
	var_date date:='2022-03-01'::date; 
begin 
	raise notice 'New OP goes below:'; 	

	loop
		delete from data_vajapora.su_churn_7_days_txn_summary
		where report_date=var_date;
		delete from data_vajapora.su_churn_7_days_txn_details
		where report_date=var_date;
	
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select * 
		from 
			(select mobile_no su_mobile_no
			from tallykhata.tk_spu_aspu_data 
			where 
				pu_type in('SPU')
				and pu_subtype is null
				and report_date=var_date-1
			) tbl1 
			
			left join 
			
			(select distinct mobile_no active_mobile_no
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime>=var_date and created_datetime<var_date+7
			) tbl2 on(tbl1.su_mobile_no=tbl2.active_mobile_no);
	
		insert into data_vajapora.su_churn_7_days_txn_summary
		select 
			var_date report_date, 
			count(case when active_mobile_no is null then su_mobile_no else null end) su_churned_in_7_day_frame 
		from 
			data_vajapora.help_a tbl1
			
			left join 
			
			(select mobile_no 
			from data_vajapora.su_churn_7_days_txn_details 
			where report_date>=var_date-7 and report_date<var_date
			) tbl2 on(tbl1.su_mobile_no=tbl2.mobile_no)
		where tbl2.mobile_no is null;
			
		insert into data_vajapora.su_churn_7_days_txn_details 
		select tbl1.* 
		from 
			(select var_date report_date, su_mobile_no mobile_no
			from data_vajapora.help_a
			where active_mobile_no is null
			) tbl1 
			
			left join 
			
			(select mobile_no 
			from data_vajapora.su_churn_7_days_txn_details 
			where report_date>=var_date-7 and report_date<var_date
			) tbl2 using(mobile_no)
		where tbl2.mobile_no is null; 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date-7 then exit; 
		end if;
	end loop;
end $$; 

select *
from data_vajapora.su_churn_7_days_txn_summary 
order by 1; 

select report_date, count(mobile_no) churned_spus
from data_vajapora.su_churn_7_days_txn_details
group by 1
order by 1; 

/*
-- txn_su_churn_7_days_open
create table data_vajapora.txn_su_churn_7_days_open_summary(report_date date, su_churned_in_7_day_frame int); 
create table data_vajapora.txn_su_churn_7_days_open_details(report_date date, mobile_no text); 
*/

do $$ 

declare 
	var_date date:='2022-03-01'::date; 
begin 
	raise notice 'New OP goes below:'; 	

	loop
		delete from data_vajapora.txn_su_churn_7_days_open_summary
		where report_date=var_date;
		delete from data_vajapora.txn_su_churn_7_days_open_details
		where report_date=var_date;
	
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select * 
		from 
			(select mobile_no su_mobile_no
			from tallykhata.tk_spu_aspu_data 
			where 
				pu_type in('SPU')
				and pu_subtype is null
				and report_date=var_date-1
			) tbl1 
			
			inner join 
			
			(select mobile_no 
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime=var_date-1
			) tbl3 on(tbl1.su_mobile_no=tbl3.mobile_no)
			
			left join 
			
			(select distinct mobile_no active_mobile_no
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date>=var_date and event_date<var_date+7
			) tbl2 on(tbl1.su_mobile_no=tbl2.active_mobile_no);
	
		insert into data_vajapora.txn_su_churn_7_days_open_summary
		select 
			var_date report_date, 
			count(case when active_mobile_no is null then su_mobile_no else null end) su_churned_in_7_day_frame 
		from 
			data_vajapora.help_a tbl1
			
			left join 
			
			(select mobile_no 
			from data_vajapora.txn_su_churn_7_days_open_details 
			where report_date>=var_date-7 and report_date<var_date
			) tbl2 on(tbl1.su_mobile_no=tbl2.mobile_no)
		where tbl2.mobile_no is null;
			
		insert into data_vajapora.txn_su_churn_7_days_open_details 
		select tbl1.* 
		from 
			(select var_date report_date, su_mobile_no mobile_no
			from data_vajapora.help_a
			where active_mobile_no is null
			) tbl1 
			
			left join 
			
			(select mobile_no 
			from data_vajapora.txn_su_churn_7_days_open_details 
			where report_date>=var_date-7 and report_date<var_date
			) tbl2 using(mobile_no)
		where tbl2.mobile_no is null; 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date-7 then exit; 
		end if;
	end loop;
end $$; 

select *
from data_vajapora.txn_su_churn_7_days_open_summary 
order by 1; 

select report_date, count(mobile_no) churned_spus
from data_vajapora.txn_su_churn_7_days_open_details
group by 1
order by 1; 

/*
-- txn_su_churn_7_days_txn
create table data_vajapora.txn_su_churn_7_days_txn_summary(report_date date, su_churned_in_7_day_frame int); 
create table data_vajapora.txn_su_churn_7_days_txn_details(report_date date, mobile_no text); 
*/

do $$ 

declare 
	var_date date:='2022-03-01'::date; 
begin 
	raise notice 'New OP goes below:'; 	

	loop
		delete from data_vajapora.txn_su_churn_7_days_txn_summary
		where report_date=var_date;
		delete from data_vajapora.txn_su_churn_7_days_txn_details
		where report_date=var_date;
	
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select * 
		from 
			(select mobile_no su_mobile_no
			from tallykhata.tk_spu_aspu_data 
			where 
				pu_type in('SPU')
				and pu_subtype is null
				and report_date=var_date-1
			) tbl1 
			
			inner join 
			
			(select mobile_no 
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime=var_date-1
			) tbl3 on(tbl1.su_mobile_no=tbl3.mobile_no)
			
			left join 
			
			(select distinct mobile_no active_mobile_no
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime>=var_date and created_datetime<var_date+7
			) tbl2 on(tbl1.su_mobile_no=tbl2.active_mobile_no);
	
		insert into data_vajapora.txn_su_churn_7_days_txn_summary
		select 
			var_date report_date, 
			count(case when active_mobile_no is null then su_mobile_no else null end) su_churned_in_7_day_frame 
		from 
			data_vajapora.help_a tbl1
			
			left join 
			
			(select mobile_no 
			from data_vajapora.txn_su_churn_7_days_txn_details 
			where report_date>=var_date-7 and report_date<var_date
			) tbl2 on(tbl1.su_mobile_no=tbl2.mobile_no)
		where tbl2.mobile_no is null;
			
		insert into data_vajapora.txn_su_churn_7_days_txn_details 
		select tbl1.* 
		from 
			(select var_date report_date, su_mobile_no mobile_no
			from data_vajapora.help_a
			where active_mobile_no is null
			) tbl1 
			
			left join 
			
			(select mobile_no 
			from data_vajapora.txn_su_churn_7_days_txn_details 
			where report_date>=var_date-7 and report_date<var_date
			) tbl2 using(mobile_no)
		where tbl2.mobile_no is null; 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date-7 then exit; 
		end if;
	end loop;
end $$; 

select *
from data_vajapora.txn_su_churn_7_days_txn_summary 
order by 1; 

select report_date, count(mobile_no) churned_spus
from data_vajapora.txn_su_churn_7_days_txn_details
group by 1
order by 1; 

-- all results
select *
from 
	(select report_date, count(mobile_no) sus
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type in('SPU')
		and pu_subtype is null
		and report_date>='2022-03-01'::date
	group by 1 
	) tbl1
	
	inner join 
	
	(select report_date, count(mobile_no) sus_txn
	from 
		(select mobile_no, report_date
		from tallykhata.tk_spu_aspu_data 
		where 
			pu_type in('SPU')
			and pu_subtype is null
			and report_date>='2022-03-01'::date
		) tbl1 
		
		inner join 
		
		(select mobile_no, created_datetime report_date
		from tallykhata.tallykhata_transacting_user_date_sequence_final 
		where created_datetime>='2022-03-01'::date
		) tbl2 using(mobile_no, report_date)
	group by 1 
	) tbl0 using(report_date)
	
	inner join 
	(select report_date, su_churned_in_7_day_frame su_churn_7_days_open from data_vajapora.su_churn_7_days_open_summary) tbl2 using (report_date)
	inner join 
	(select report_date, su_churned_in_7_day_frame su_churn_7_days_txn from data_vajapora.su_churn_7_days_txn_summary) tbl3 using (report_date)
	inner join 
	(select report_date, su_churned_in_7_day_frame txn_su_churn_7_days_open from data_vajapora.txn_su_churn_7_days_open_summary) tbl4 using (report_date)
	inner join 
	(select report_date, su_churned_in_7_day_frame txn_su_churn_7_days_txn from data_vajapora.txn_su_churn_7_days_txn_summary) tbl5 using (report_date)
	inner join 
	(select 
		report_date, 
		count(distinct tbl1.mobile_no) su_churn_7_days_overall, 
		count(distinct tbl2.mobile_no) su_winback
	from 
		(select * from data_vajapora.su_churn_7_days_open_details
		union all
		select * from data_vajapora.su_churn_7_days_txn_details
		union all
		select * from data_vajapora.txn_su_churn_7_days_open_details
		union all
		select * from data_vajapora.txn_su_churn_7_days_txn_details
		) tbl1
		
		left join 
		
		(select mobile_no, max(report_date) return_date
		from 
			(select mobile_no, created_datetime report_date
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			union all
			select mobile_no, event_date report_date
			from tallykhata.tallykhata_user_date_sequence_final
			) tbl1 
		group by 1
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.report_date<tbl2.return_date)
	group by 1
	) tbl6 using(report_date)
order by 1; 

-- list
with 
	temp_table as
	(select *
	from data_vajapora.txn_su_churn_7_days_open_details
	where 
		report_date in(
		'07-Mar-22','08-Mar-22','09-Mar-22',
		'04-Apr-22','05-Apr-22','06-Apr-22',
		'23-May-22','24-May-22','25-May-22'
		)
	) 
	
select report_date, mobile_no, last_txn_date, case when last_txn_date<report_date then 'no' else null end if_returned_to_txn, first_txn_date_after_return, last_open_date                       
from 
	temp_table tbl1 
	
	left join 
		
	(select mobile_no, max(created_datetime) last_txn_date 
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	group by 1
	) tbl2 using(mobile_no) 
	
	left join 
	
	(select mobile_no, max(event_date) last_open_date 
	from tallykhata.tallykhata_user_date_sequence_final
	group by 1
	) tbl3 using(mobile_no) 

	left join 
	
	(select tbl1.mobile_no, min(created_datetime) first_txn_date_after_return
	from 
		temp_table tbl1 
		
		left join 
			
		(select mobile_no, created_datetime
		from tallykhata.tallykhata_transacting_user_date_sequence_final
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.report_date<tbl2.created_datetime)
	group by 1
	) tbl4 using(mobile_no); 
