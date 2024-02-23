CREATE TEMPORARY TABLE temp_delete_me (page_id INT);
INSERT INTO temp_delete_me (page_id)
	SELECT page_id FROM ticks_check
	WHERE stars_difference <> 0
    OR suggested_grade_difference NOT IN (0, -1)
    OR to_do_list_difference <> 0
    OR ticks_difference <> 0;

DELETE FROM stats_ratings WHERE page_id IN (SELECT page_id FROM temp_delete_me);
DELETE FROM stats_stars WHERE page_id IN (SELECT page_id FROM temp_delete_me);
DELETE FROM stats_ticks WHERE page_id IN (SELECT page_id FROM temp_delete_me);
DELETE FROM stats_todos WHERE page_id IN (SELECT page_id FROM temp_delete_me);
DELETE FROM stats_count WHERE page_id IN (SELECT page_id FROM temp_delete_me);

DROP TEMPORARY TABLE temp_delete_me;