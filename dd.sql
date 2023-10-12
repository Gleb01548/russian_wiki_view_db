select ROW_NUMBER() over(
        order by sum_views DESC
    ),
    page_name,
    sum_views
from (
        select page_name,
            datetime::date,
            SUM(page_view_count) as sum_views
        from resource.data_views
        where datetime::date = '2022-01-01'
        group by page_name,
            datetime::date
        order by sum_views desc
        limit 30
    ) as table1