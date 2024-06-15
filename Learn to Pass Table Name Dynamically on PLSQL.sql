/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): tutorial from Samir Kumar Ghosh
*/

do $$
	
declare 
	var_tbl varchar; 
	var_sql varchar; 
	var_n int; 
begin 
	raise notice 'New OP goes below:'; 
	
	var_tbl:='tallykhata_fact_info_final';
	raise notice '%', var_tbl; 
	
	drop table if exists data_vajapora.help_a; 
	var_sql:=
	'create table data_vajapora.help_a as 
	select *
	from tallykhata.'||var_tbl||' -- passing here
	limit 500';
	execute var_sql; 
end $$; 

select *
from data_vajapora.help_a; 
