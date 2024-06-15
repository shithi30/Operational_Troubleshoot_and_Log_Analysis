/*
- Viz: https://docs.google.com/spreadsheets/d/14isAzMutxpj3MqjMxvvLSFjaDSuV-YcVNXZBNtPVjCU/edit?pli=1#gid=0
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: http://localhost:8888/notebooks/Import%20from%20csv%20to%20DB/Address%20from%20Lat-Lng.ipynb
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 2 coordinates were found in Azimpur.  
*/

-- erroneous locations in derived table
select mobile mobile_no, lat::numeric, lng::numeric, concat('https://maps.google.com/?q=',lat,',',lng) location_url
from tallykhata.tallykhata_clients_location_info
where mobile in('01811448475', '01793777238');
	
-- all locations in main table
select id, tallykhata_user_id, lat::text, long::text lng, created_at
from public.locations 
where tallykhata_user_id in(select tallykhata_user_id from public.register_usermobile where mobile_number in('01811448475', '01793777238')); 

-- all addresses in main table 
select 
	*, 
	trim('"' from split_part(split_part(replace(street_address, '''', '"'), '"state_district": ', 2), ',', 1)) district,
	trim('"' from split_part(split_part(replace(street_address, '''', '"'), '"city": ', 2), ',', 1)) city,
	trim('"' from split_part(split_part(replace(street_address, '''', '"'), '"suburb": ', 2), ',', 1)) suburb, 
	trim('"' from split_part(split_part(replace(street_address, '''', '"'), '"quarter": ', 2), ',', 1)) quarter, 
	trim('"' from split_part(split_part(replace(street_address, '''', '"'), '"road": ', 2), ',', 1)) road
from data_vajapora.help_c; 