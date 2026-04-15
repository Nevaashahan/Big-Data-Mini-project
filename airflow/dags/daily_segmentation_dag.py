from datetime import datetime, timedelta
import os

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "ecommerce_analytics"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}
REPORT_PATH = "/opt/airflow/reports/daily_summary.txt"


def get_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)



def build_daily_outputs():
    report_date = (datetime.utcnow() - timedelta(days=1)).date()

    user_segment_sql = """
        INSERT INTO daily_user_segments (
            report_date, user_id, total_views, total_add_to_cart, total_purchases, segment
        )
        SELECT
            %(report_date)s AS report_date,
            user_id,
            COUNT(*) FILTER (WHERE event_type = 'view') AS total_views,
            COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS total_add_to_cart,
            COUNT(*) FILTER (WHERE event_type = 'purchase') AS total_purchases,
            CASE
                WHEN COUNT(*) FILTER (WHERE event_type = 'purchase') > 0 THEN 'Buyer'
                WHEN COUNT(*) FILTER (WHERE event_type = 'add_to_cart') > 0 THEN 'Interested Buyer'
                ELSE 'Window Shopper'
            END AS segment
        FROM clickstream_events
        WHERE DATE(event_time) = %(report_date)s
        GROUP BY user_id
        ON CONFLICT (report_date, user_id)
        DO UPDATE SET
            total_views = EXCLUDED.total_views,
            total_add_to_cart = EXCLUDED.total_add_to_cart,
            total_purchases = EXCLUDED.total_purchases,
            segment = EXCLUDED.segment
    """

    daily_top_sql = """
        WITH ranked AS (
            SELECT
                %(report_date)s AS report_date,
                product_id,
                COUNT(*) FILTER (WHERE event_type = 'view') AS total_views,
                ROW_NUMBER() OVER (
                    ORDER BY COUNT(*) FILTER (WHERE event_type = 'view') DESC, product_id ASC
                ) AS rank_no
            FROM clickstream_events
            WHERE DATE(event_time) = %(report_date)s
            GROUP BY product_id
        )
        INSERT INTO daily_top_products (report_date, rank_no, product_id, total_views)
        SELECT report_date, rank_no, product_id, total_views
        FROM ranked
        WHERE rank_no <= 5
        ON CONFLICT (report_date, rank_no)
        DO UPDATE SET
            product_id = EXCLUDED.product_id,
            total_views = EXCLUDED.total_views
    """

    conversion_sql = """
        INSERT INTO daily_conversion_report (
            report_date, category, total_views, total_purchases, conversion_rate
        )
        SELECT
            %(report_date)s AS report_date,
            category,
            COUNT(*) FILTER (WHERE event_type = 'view') AS total_views,
            COUNT(*) FILTER (WHERE event_type = 'purchase') AS total_purchases,
            CASE
                WHEN COUNT(*) FILTER (WHERE event_type = 'view') = 0 THEN 0
                ELSE ROUND(
                    (COUNT(*) FILTER (WHERE event_type = 'purchase'))::numeric
                    / NULLIF(COUNT(*) FILTER (WHERE event_type = 'view'), 0),
                    4
                )
            END AS conversion_rate
        FROM clickstream_events
        WHERE DATE(event_time) = %(report_date)s
        GROUP BY category
        ON CONFLICT (report_date, category)
        DO UPDATE SET
            total_views = EXCLUDED.total_views,
            total_purchases = EXCLUDED.total_purchases,
            conversion_rate = EXCLUDED.conversion_rate
    """

    summary_sql = """
        SELECT rank_no, product_id, total_views
        FROM daily_top_products
        WHERE report_date = %(report_date)s
        ORDER BY rank_no ASC
    """

    segment_count_sql = """
        SELECT segment, COUNT(*)
        FROM daily_user_segments
        WHERE report_date = %(report_date)s
        GROUP BY segment
        ORDER BY segment ASC
    """

    conversion_summary_sql = """
        SELECT category, total_views, total_purchases, conversion_rate
        FROM daily_conversion_report
        WHERE report_date = %(report_date)s
        ORDER BY conversion_rate DESC, category ASC
    """

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(user_segment_sql, {"report_date": report_date})
            cursor.execute(daily_top_sql, {"report_date": report_date})
            cursor.execute(conversion_sql, {"report_date": report_date})
            conn.commit()

            cursor.execute(summary_sql, {"report_date": report_date})
            top_rows = cursor.fetchall()

            cursor.execute(segment_count_sql, {"report_date": report_date})
            segment_rows = cursor.fetchall()

            cursor.execute(conversion_summary_sql, {"report_date": report_date})
            conversion_rows = cursor.fetchall()

        lines = [
            "E-Commerce Daily Summary",
            f"Report date: {report_date}",
            f"Generated at: {datetime.utcnow().isoformat()} UTC",
            "",
            "Top 5 Most Viewed Products",
            "--------------------------",
        ]

        if top_rows:
            for rank_no, product_id, total_views in top_rows:
                lines.append(f"{rank_no}. {product_id} - {total_views} views")
        else:
            lines.append("No clickstream data found for this date.")

        lines.extend(["", "User Segmentation", "-----------------"])
        if segment_rows:
            for segment, count_value in segment_rows:
                lines.append(f"{segment}: {count_value}")
        else:
            lines.append("No user segments available.")

        lines.extend(["", "Conversion by Category", "----------------------"])
        if conversion_rows:
            for category, total_views, total_purchases, conversion_rate in conversion_rows:
                lines.append(
                    f"{category}: views={total_views}, purchases={total_purchases}, conversion_rate={conversion_rate}"
                )
        else:
            lines.append("No conversion data available.")

        with open(REPORT_PATH, "w", encoding="utf-8") as file:
            file.write("\n".join(lines))
    finally:
        conn.close()


with DAG(
    dag_id="daily_user_segmentation_pipeline",
    start_date=datetime(2026, 4, 15),
    schedule="@daily",
    catchup=False,
    tags=["ecommerce", "analytics", "postgresql"],
) as dag:
    build_daily_summary = PythonOperator(
        task_id="build_daily_summary",
        python_callable=build_daily_outputs,
    )

    build_daily_summary
