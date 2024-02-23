CREATE OR REPLACE VIEW route_type AS
	SELECT
		page_id,
		SUBSTRING_INDEX(route_type, ', ', 1) AS route_type
	FROM mp_route_info
UNION
	SELECT
		page_id,
		SUBSTRING_INDEX(SUBSTRING_INDEX(route_type, ', ', 2), ', ', -1) AS route_type
	FROM mp_route_info
UNION
	SELECT
		page_id,
		SUBSTRING_INDEX(SUBSTRING_INDEX(route_type, ', ', 3), ', ', -1) AS route_type
	FROM mp_route_info
UNION
	SELECT
		page_id,
		SUBSTRING_INDEX(SUBSTRING_INDEX(route_type, ', ', 4), ', ', -1) AS route_type
	FROM mp_route_info
UNION
	SELECT
		page_id,
		SUBSTRING_INDEX(SUBSTRING_INDEX(route_type, ', ', 5), ', ', -1) AS route_type
	FROM mp_route_info
UNION
	SELECT
		page_id,
		SUBSTRING_INDEX(route_type, ', ', -1) AS route_type
	FROM mp_route_info;

SELECT * FROM route_type
	ORDER BY page_id;
    
SELECT route_type, count(route_type)
	FROM route_type
    GROUP BY route_type
