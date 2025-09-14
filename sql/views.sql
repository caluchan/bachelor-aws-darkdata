CREATE OR REPLACE VIEW v_darkdata_stats_rental AS
select
    d.tablename AS tablename,
    d.gesamt_anzahl AS gesamt_anzahl,
    d.total_size_in_bytes AS total_size_in_bytes,
    d.alte_eintraege AS alte_eintraege,
    round(((d.alte_eintraege / d.gesamt_anzahl) * d.total_size_in_bytes), 0) AS alte_bytes,
    round((d.total_size_in_bytes - ((d.alte_eintraege / d.gesamt_anzahl) * d.total_size_in_bytes)), 0) AS neue_bytes
from (
    select 
    	ts.TABLE_NAME AS tablename,
        round((ts.DATA_LENGTH + ts.INDEX_LENGTH), 2) AS total_size_in_bytes,
        (select count(*) from rental) AS gesamt_anzahl, 
        (Select count(*) from rental where (last_update < (now() - interval 60 day))) AS alte_eintraege
    from information_schema.TABLES ts
    where ts.TABLE_SCHEMA = 'sakila'
    and ts.TABLE_NAME = 'rental'
) d;

CREATE OR REPLACE VIEW v_darkdata_stats_address AS
select
    d.tablename AS tablename,
    d.gesamt_anzahl AS gesamt_anzahl,
    d.total_size_in_bytes AS total_size_in_bytes,
    d.alte_eintraege AS alte_eintraege,
    round(((d.alte_eintraege / d.gesamt_anzahl) * d.total_size_in_bytes), 0) AS alte_bytes,
    round((d.total_size_in_bytes - ((d.alte_eintraege / d.gesamt_anzahl) * d.total_size_in_bytes)), 0) AS neue_bytes
from (
    select 
    	ts.TABLE_NAME AS tablename,
        round((ts.DATA_LENGTH + ts.INDEX_LENGTH), 2) AS total_size_in_bytes,
        (select count(*) from address) AS gesamt_anzahl, 
        (Select count(*) from address where last_update < (now() - interval 60 day)) AS alte_eintraege
    from information_schema.TABLES ts
    where ts.TABLE_SCHEMA = 'sakila'
    and ts.TABLE_NAME = 'address'
) d;

CREATE OR REPLACE VIEW v_doppelte_eintraege AS
select 
  'address' as tablename,
	sum(a.haeufigkeit_einzelne_addresse - 1) as doppelte_eintraege,
	ddstats.gesamt_anzahl as gesamt_anzahl,
	ddstats.gesamt_anzahl - sum(a.haeufigkeit_einzelne_addresse - 1)  as einzigartige_eintraege,
	round(sum(a.haeufigkeit_einzelne_addresse) / ddstats.gesamt_anzahl * ddstats.total_size_in_bytes, 0) as doppelte_bytes,
	ddstats.total_size_in_bytes
from (
	SELECT address, COUNT(*) as haeufigkeit_einzelne_addresse
	FROM address
	GROUP BY address
	HAVING COUNT(*) > 1
) a
join v_darkdata_stats_address ddstats on ddstats.tablename = 'address';