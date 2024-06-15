do $$ 

declare 
	var_month text;
	var_start_date date; 
	var_end_date date;
	var_loop int:=23; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- months
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select *, row_number() over(order by year_month) seq
	from 
		(select distinct left(hi::text, 7) year_month
		from (select current_date-generate_series(1, current_date-'2020-05-01'::date) hi) tbl1 
		order by 1
		) tbl1; 
	
	loop
		-- month start and end
		var_month:=(select year_month from data_vajapora.help_a where seq=var_loop); 
		var_start_date:=concat(var_month, '-01')::date-1; 
		var_end_date:=concat(left((var_start_date+37)::text, 7), '-01')::date; 
	
		delete from data_vajapora.monthly_message_activity_1
		where year_month=var_month; 
	
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select 
			mobile_no, 
			count(case when event_name='in_app_message_link_tap' then id else null end) in_app_message_link_tap_activity, 
			count(case when event_name='in_app_message_open' then id else null end) in_app_message_open_activity, 
			count(case when event_name='in_app_message_close' then id else null end) in_app_message_close_activity, 
			count(case when event_name='inbox_message_open' then id else null end) inbox_message_open_activity, 
			var_month year_month
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_name in('in_app_message_link_tap', 'in_app_message_open', 'in_app_message_close', 'inbox_message_open')
			and event_date>var_start_date and event_date<var_end_date
		group by 1; 
	
		insert into data_vajapora.monthly_message_activity_1
		select * 
		from data_vajapora.help_b; 
		
		commit; 
		raise notice 'Data generated for: %, from % to %', var_month, var_start_date, var_end_date; 
		var_loop:=var_loop+1; 
		if var_loop=(select max(seq) from data_vajapora.help_a)+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.monthly_message_activity_1;  

-- for Khalid Bh. 
select case when tg is not null then tg else 'others' end tg, count(*) users
from 
	(select distinct mobile_no 
	from tallykhata.tallykhata_fact_info_final 
	where txn_timestamp>='2022-06-11 16:30:00' and txn_timestamp<'2022-06-11 17:00:00'
	) tbl1 
	
	left join 
	
	(select 
		mobile_no,
		max(
			case 
				when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
				when tg in('LTUCb','LTUTa') then 'LTU'
				when tg in('NB0','NN1','NN2-6') then 'NN'
				when tg in('NT--') then 'NT'
				when tg in('PSU') then 'PSU'
				when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
				when tg in('SPU') then 'SU'
				when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie'
				else null
			end
		) tg
	from cjm_segmentation.retained_users 
	where report_date=current_date
	group by 1
	) tbl2 using(mobile_no)
group by 1
order by 2 desc; 

select case when tg is not null then tg else 'others' end tg, mobile_no
from 
	(select distinct mobile_no 
	from tallykhata.tallykhata_fact_info_final 
	where txn_timestamp>='2022-06-11 16:30:00' and txn_timestamp<'2022-06-11 17:00:00'
	) tbl1 
	
	left join 
	
	(select 
		mobile_no,
		max(
			case 
				when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
				when tg in('LTUCb','LTUTa') then 'LTU'
				when tg in('NB0','NN1','NN2-6') then 'NN'
				when tg in('NT--') then 'NT'
				when tg in('PSU') then 'PSU'
				when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
				when tg in('SPU') then 'SU'
				when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie'
				else null
			end
		) tg
	from cjm_segmentation.retained_users 
	where report_date=current_date
	group by 1
	) tbl2 using(mobile_no)
order by 1; 
