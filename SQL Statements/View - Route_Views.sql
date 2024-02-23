CREATE OR REPLACE VIEW mp_route_info_export AS
SELECT
	mr.page_id,
    mr.date_grabbed,
    mr.name,
    mr.fa,
    mr.distance_ft,
    mr.pitches,
    mr.fixed_pieces,
    mr.votes,
    mr.views,
    mr.date_added,
    mr.shared_by,
    mr.latitude,
    mr.longitude,
    sc.stars,
    sc.ratings,
    sc.todos,
    sc.ticks
    
FROM
	mp_route_info mr
    
LEFT JOIN
	stats_count sc
ON
	mr.page_id = sc.page_id
    