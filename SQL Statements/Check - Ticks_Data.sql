CREATE OR REPLACE VIEW ticks_check AS
WITH aggregated_counts AS (
    SELECT
        sc.page_id,
        COALESCE(COUNT(ss.page_id), 0) AS suggested_stars_count,
        COALESCE(COUNT(sg.page_id), 0) AS suggested_grade_count,
        COALESCE(COUNT(td.page_id), 0) AS to_do_list_count,
        COALESCE(COUNT(ti.page_id), 0) AS ticks_count
    FROM stats_count sc
    LEFT JOIN stats_stars ss ON sc.page_id = ss.page_id
    LEFT JOIN stats_ratings sg ON sc.page_id = sg.page_id
    LEFT JOIN stats_todos td ON sc.page_id = td.page_id
    LEFT JOIN stats_ticks ti ON sc.page_id = ti.page_id
    GROUP BY sc.page_id
)
SELECT
    sc.page_id,
    (COALESCE(ac.suggested_stars_count, 0) - sc.stars) AS stars_difference,
    (COALESCE(ac.suggested_grade_count, 0) - sc.ratings) AS suggested_grade_difference,
    (COALESCE(ac.to_do_list_count, 0) - sc.todos) AS to_do_list_difference,
    (COALESCE(ac.ticks_count, 0) - sc.ticks) AS ticks_difference,
    (COALESCE(ac.suggested_stars_count, 0) - sc.stars + COALESCE(ac.suggested_grade_count, 0) - sc.ratings + COALESCE(ac.to_do_list_count, 0) - sc.todos + COALESCE(ac.ticks_count, 0) - sc.ticks) AS total_difference
FROM stats_count sc
LEFT JOIN aggregated_counts ac ON sc.page_id = ac.page_id
WHERE (COALESCE(ac.suggested_stars_count, 0) - sc.stars + COALESCE(ac.suggested_grade_count, 0) - sc.ratings + COALESCE(ac.to_do_list_count, 0) - sc.todos + COALESCE(ac.ticks_count, 0) - sc.ticks) <> 0;
;
SELECT * FROM ticks_check