INSERT INTO address (address, district, city_id,postal_code,phone ,location,last_update)
SELECT address, district, city_id,postal_code,phone,location,last_update
FROM address
ORDER BY RAND()
LIMIT 258; 

SELECT address, COUNT(*) as count
FROM address
GROUP BY address
HAVING COUNT(*) > 1;