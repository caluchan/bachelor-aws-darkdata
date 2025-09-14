SET @start_date = NOW() - INTERVAL 365 DAY;
UPDATE rental SET last_update = DATE_ADD(@start_date, INTERVAL FLOOR(RAND() * 365) DAY);
UPDATE address SET last_update = DATE_ADD(@start_date, INTERVAL FLOOR(RAND() * 365) DAY);