CREATE OR REPLACE VIEW route_views_per_month AS
WITH RankedRoutes AS (
    SELECT
        page_id,
        date_added,
        views AS total_views,
        TIMESTAMPDIFF(MONTH, date_added, "2023-11-17") +
            DATEDIFF(
                "2023-11-17",
                date_added + INTERVAL TIMESTAMPDIFF(MONTH, date_added, "2023-11-17") MONTH
            ) / DATEDIFF(
                    date_added + INTERVAL TIMESTAMPDIFF(MONTH, date_added, "2023-11-17") + 1 MONTH,
                    date_added + INTERVAL TIMESTAMPDIFF(MONTH, date_added, "2023-11-17") MONTH
                ) AS month_count,
        views / (
                TIMESTAMPDIFF(MONTH, date_added, "2023-11-17") +
                DATEDIFF(
                    "2023-11-17",
                    date_added + INTERVAL TIMESTAMPDIFF(MONTH, date_added, "2023-11-17") MONTH
                ) / DATEDIFF(
                        date_added + INTERVAL TIMESTAMPDIFF(MONTH, date_added, "2023-11-17") + 1 MONTH,
                        date_added + INTERVAL TIMESTAMPDIFF(MONTH, date_added, "2023-11-17") MONTH
                    )
            ) AS views_per_month
    FROM
        mp_route_info
)
SELECT
    page_id,
    date_added,
    total_views,
    month_count,
    views_per_month,
    RANK() OVER (ORDER BY views_per_month DESC) AS `rank`
FROM
    RankedRoutes;

SELECT * FROM route_views_per_month