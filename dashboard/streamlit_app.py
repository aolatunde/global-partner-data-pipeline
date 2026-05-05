import os
from datetime import timedelta

import pandas as pd
import streamlit as st
import plotly.express as px
from pyathena import connect


st.set_page_config(
    page_title="Global Partner Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

DEFAULT_REGION = os.getenv("AWS_REGION", "us-east-1")
DEFAULT_DATABASE = os.getenv("ATHENA_DATABASE", "global_partner_gold")
DEFAULT_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
DEFAULT_OUTPUT_LOCATION = os.getenv("ATHENA_OUTPUT_LOCATION")

if not DEFAULT_OUTPUT_LOCATION:
    st.error("Athena output location is not configured.")
    st.stop()

# Current Gold tables contain restaurant_id. If a true restaurant_name column is
# added later, set RESTAURANT_NAME_SQL=restaurant_name.
ALLOWED_RESTAURANT_NAME_COLUMNS = ("restaurant_id", "restaurant_name")
RESTAURANT_NAME_SQL_DEFAULT = os.getenv("RESTAURANT_NAME_SQL", "restaurant_id")
if RESTAURANT_NAME_SQL_DEFAULT not in ALLOWED_RESTAURANT_NAME_COLUMNS:
    RESTAURANT_NAME_SQL_DEFAULT = "restaurant_id"

TABLE_DAILY_SALES = "gold_daily_restaurant_sales"
TABLE_ITEM_PERFORMANCE = "gold_item_performance"
TABLE_LOYALTY = "gold_loyalty_sales_summary"
TABLE_ORDER_SUMMARY = "gold_order_summary"


@st.cache_resource(show_spinner=False)
def get_athena_connection(region_name: str, database: str, output_location: str, workgroup: str):
    return connect(
        s3_staging_dir=output_location,
        region_name=region_name,
        schema_name=database,
        work_group=workgroup
    )


@st.cache_data(ttl=900, show_spinner=True)
def run_query(query: str, region_name: str, database: str, output_location: str, workgroup: str) -> pd.DataFrame:
    conn = get_athena_connection(region_name, database, output_location, workgroup)
    return pd.read_sql(query, conn)


def sql_list(values):
    safe_values = [str(v).replace("'", "''") for v in values]
    return ", ".join([f"'{v}'" for v in safe_values])


def build_filters(start_date, end_date, restaurant_names, restaurant_name_sql):
    filters = [
        f"order_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
    ]

    if restaurant_names:
        filters.append(f"CAST({restaurant_name_sql} AS VARCHAR) IN ({sql_list(restaurant_names)})")

    return " AND ".join(filters)


def safe_divide(numerator, denominator):
    if denominator in (0, None) or pd.isna(denominator):
        return 0
    return numerator / denominator


def format_currency(value):
    if value is None or pd.isna(value):
        value = 0
    return f"${value:,.2f}"


def format_number(value):
    if value is None or pd.isna(value):
        value = 0
    return f"{value:,.0f}"


st.sidebar.title("Global Partner Dashboard")
st.sidebar.caption("Athena-backed analytics app")

with st.sidebar.expander("Connection Settings", expanded=False):
    aws_region = st.text_input("AWS Region", DEFAULT_REGION)
    athena_database = st.text_input("Athena Database", DEFAULT_DATABASE)
    athena_workgroup = st.text_input("Athena Workgroup", DEFAULT_WORKGROUP)
    athena_output_location = DEFAULT_OUTPUT_LOCATION
    restaurant_name_sql = st.selectbox(
        "Restaurant Name Column",
        ALLOWED_RESTAURANT_NAME_COLUMNS,
        index=ALLOWED_RESTAURANT_NAME_COLUMNS.index(RESTAURANT_NAME_SQL_DEFAULT),
        help="Use restaurant_id for current Gold tables, or restaurant_name if you later add that column."
    )

st.sidebar.markdown("---")

try:
    min_max_query = f"""
        SELECT
            MIN(order_date) AS min_date,
            MAX(order_date) AS max_date
        FROM {TABLE_DAILY_SALES}
    """

    date_bounds = run_query(
        min_max_query,
        aws_region,
        athena_database,
        athena_output_location,
        athena_workgroup
    )

    if date_bounds.empty or pd.isna(date_bounds.loc[0, "min_date"]):
        st.warning("No data found in gold_daily_restaurant_sales. Confirm Athena partitions and table location.")
        st.stop()

    min_date = pd.to_datetime(date_bounds.loc[0, "min_date"]).date()
    max_date = pd.to_datetime(date_bounds.loc[0, "max_date"]).date()

    restaurant_query = f"""
        SELECT DISTINCT CAST({restaurant_name_sql} AS VARCHAR) AS restaurant_name
        FROM {TABLE_DAILY_SALES}
        WHERE {restaurant_name_sql} IS NOT NULL
        ORDER BY restaurant_name
    """

    restaurants_df = run_query(
        restaurant_query,
        aws_region,
        athena_database,
        athena_output_location,
        athena_workgroup
    )

except Exception:
    st.error("Unable to connect to Athena or load dashboard metadata.")
    st.stop()


st.sidebar.subheader("Filters")

default_start = max(min_date, max_date - timedelta(days=90))

selected_date_range = st.sidebar.date_input(
    "Order Date Range",
    value=(default_start, max_date),
    min_value=min_date,
    max_value=max_date
)

if isinstance(selected_date_range, tuple) and len(selected_date_range) == 2:
    start_date, end_date = selected_date_range
else:
    start_date, end_date = default_start, max_date

restaurant_options = restaurants_df["restaurant_name"].dropna().astype(str).tolist()
selected_restaurant_names = st.sidebar.multiselect(
    "Restaurant Name",
    restaurant_options,
    default=[]
)

where_clause = build_filters(
    start_date=start_date,
    end_date=end_date,
    restaurant_names=selected_restaurant_names,
    restaurant_name_sql=restaurant_name_sql
)


daily_sales_query = f"""
    SELECT
        order_date,
        CAST({restaurant_name_sql} AS VARCHAR) AS restaurant_name,
        currency,
        total_orders,
        distinct_customers,
        total_line_items,
        total_quantity_sold,
        base_item_revenue,
        option_revenue,
        gross_revenue,
        loyalty_revenue,
        non_loyalty_revenue,
        average_order_value,
        revenue_per_customer,
        day_of_week,
        week,
        month,
        year,
        is_weekend,
        is_holiday,
        holiday_name
    FROM {TABLE_DAILY_SALES}
    WHERE {where_clause}
"""

item_query = f"""
    SELECT
        order_date,
        CAST({restaurant_name_sql} AS VARCHAR) AS restaurant_name,
        currency,
        item_category,
        item_name,
        distinct_orders,
        distinct_customers,
        total_quantity_sold,
        item_base_revenue,
        item_option_revenue,
        item_total_revenue,
        avg_item_price,
        loyalty_item_revenue,
        non_loyalty_item_revenue,
        avg_quantity_per_order
    FROM {TABLE_ITEM_PERFORMANCE}
    WHERE {where_clause}
"""

loyalty_query = f"""
    SELECT
        order_date,
        CAST({restaurant_name_sql} AS VARCHAR) AS restaurant_name,
        currency,
        loyalty_segment,
        total_orders,
        distinct_customers,
        total_item_quantity,
        base_item_revenue,
        option_revenue,
        gross_revenue,
        average_order_value,
        revenue_per_customer
    FROM {TABLE_LOYALTY}
    WHERE {where_clause}
"""

order_query = f"""
    SELECT
        order_date,
        CAST({restaurant_name_sql} AS VARCHAR) AS restaurant_name,
        order_id,
        user_id,
        currency,
        is_loyalty,
        order_created_ts,
        app_name,
        distinct_line_item_count,
        total_item_quantity,
        order_base_item_total,
        order_options_total,
        order_total_amount,
        distinct_item_count,
        avg_revenue_per_line_item
    FROM {TABLE_ORDER_SUMMARY}
    WHERE {where_clause}
    ORDER BY order_date DESC
    LIMIT 1000
"""

try:
    daily_sales = run_query(
        daily_sales_query,
        aws_region,
        athena_database,
        athena_output_location,
        athena_workgroup
    )
    item_perf = run_query(
        item_query,
        aws_region,
        athena_database,
        athena_output_location,
        athena_workgroup
    )
    loyalty = run_query(
        loyalty_query,
        aws_region,
        athena_database,
        athena_output_location,
        athena_workgroup
    )
    orders = run_query(
        order_query,
        aws_region,
        athena_database,
        athena_output_location,
        athena_workgroup
    )

except Exception:
    st.error("Unable to load data from analytics service. Please try again later.")
    st.stop()


st.title("Global Partner Analytics Dashboard")

if daily_sales.empty:
    st.warning("No records match the selected filters.")
    st.stop()

daily_sales["order_date"] = pd.to_datetime(daily_sales["order_date"])
if not item_perf.empty:
    item_perf["order_date"] = pd.to_datetime(item_perf["order_date"])
if not loyalty.empty:
    loyalty["order_date"] = pd.to_datetime(loyalty["order_date"])
if not orders.empty:
    orders["order_date"] = pd.to_datetime(orders["order_date"])


total_revenue = daily_sales["gross_revenue"].sum()
total_orders = daily_sales["total_orders"].sum()
distinct_customers = daily_sales["distinct_customers"].sum()
total_quantity = daily_sales["total_quantity_sold"].sum()
avg_order_value = safe_divide(total_revenue, total_orders)
option_revenue = daily_sales["option_revenue"].sum()
option_revenue_pct = safe_divide(option_revenue, total_revenue)
loyalty_revenue = daily_sales["loyalty_revenue"].sum()
loyalty_revenue_pct = safe_divide(loyalty_revenue, total_revenue)

st.markdown("### Executive Overview")

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Gross Revenue", format_currency(total_revenue))
kpi2.metric("Total Orders", format_number(total_orders))
kpi3.metric("Average Order Value", format_currency(avg_order_value))
kpi4.metric("Distinct Customers", format_number(distinct_customers))

kpi5, kpi6, kpi7, kpi8 = st.columns(4)
kpi5.metric("Quantity Sold", format_number(total_quantity))
kpi6.metric("Option Revenue", format_currency(option_revenue))
kpi7.metric("Option Revenue %", f"{option_revenue_pct:.1%}")
kpi8.metric("Loyalty Revenue %", f"{loyalty_revenue_pct:.1%}")


tab_overview, tab_restaurant, tab_items, tab_loyalty, tab_orders = st.tabs([
    "Executive Overview",
    "Restaurant Performance",
    "Item Performance",
    "Loyalty Analysis",
    "Order Drilldown"
])


with tab_overview:
    st.markdown("### Revenue and Order Trends")

    daily_trend = (
        daily_sales.groupby("order_date", as_index=False)
        .agg(
            gross_revenue=("gross_revenue", "sum"),
            total_orders=("total_orders", "sum"),
            distinct_customers=("distinct_customers", "sum")
        )
    )
    daily_trend["average_order_value"] = (
        daily_trend["gross_revenue"] / daily_trend["total_orders"].replace(0, pd.NA)
    ).fillna(0)

    c1, c2 = st.columns(2)

    with c1:
        fig = px.line(
            daily_trend,
            x="order_date",
            y="gross_revenue",
            title="Gross Revenue Over Time",
            markers=True
        )
        fig.update_layout(yaxis_title="Gross Revenue", xaxis_title="Order Date")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.line(
            daily_trend,
            x="order_date",
            y="total_orders",
            title="Total Orders Over Time",
            markers=True
        )
        fig.update_layout(yaxis_title="Orders", xaxis_title="Order Date")
        st.plotly_chart(fig, use_container_width=True)

    c3, c4 = st.columns(2)

    with c3:
        fig = px.line(
            daily_trend,
            x="order_date",
            y="average_order_value",
            title="Average Order Value Over Time",
            markers=True
        )
        fig.update_layout(yaxis_title="Average Order Value", xaxis_title="Order Date")
        st.plotly_chart(fig, use_container_width=True)

    with c4:
        revenue_mix = pd.DataFrame({
            "Segment": ["Loyalty Revenue", "Non-Loyalty Revenue", "Option Revenue", "Base Item Revenue"],
            "Revenue": [
                daily_sales["loyalty_revenue"].sum(),
                daily_sales["non_loyalty_revenue"].sum(),
                daily_sales["option_revenue"].sum(),
                daily_sales["base_item_revenue"].sum()
            ]
        })
        fig = px.bar(
            revenue_mix,
            x="Segment",
            y="Revenue",
            title="Revenue Mix"
        )
        fig.update_layout(xaxis_title="", yaxis_title="Revenue")
        st.plotly_chart(fig, use_container_width=True)


with tab_restaurant:
    st.markdown("### Restaurant Performance")

    restaurant_summary = (
        daily_sales.groupby(["restaurant_name"], as_index=False)
        .agg(
            gross_revenue=("gross_revenue", "sum"),
            total_orders=("total_orders", "sum"),
            distinct_customers=("distinct_customers", "sum"),
            total_quantity_sold=("total_quantity_sold", "sum"),
            loyalty_revenue=("loyalty_revenue", "sum"),
            non_loyalty_revenue=("non_loyalty_revenue", "sum")
        )
    )
    restaurant_summary["average_order_value"] = (
        restaurant_summary["gross_revenue"] /
        restaurant_summary["total_orders"].replace(0, pd.NA)
    ).fillna(0)

    c1, c2 = st.columns(2)

    with c1:
        top_restaurants = restaurant_summary.sort_values("gross_revenue", ascending=False).head(20)
        fig = px.bar(
            top_restaurants,
            x="restaurant_name",
            y="gross_revenue",
            title="Top Restaurants by Revenue"
        )
        fig.update_layout(xaxis_title="Restaurant Name", yaxis_title="Revenue")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        top_orders = restaurant_summary.sort_values("total_orders", ascending=False).head(20)
        fig = px.bar(
            top_orders,
            x="restaurant_name",
            y="total_orders",
            title="Top Restaurants by Orders"
        )
        fig.update_layout(xaxis_title="Restaurant Name", yaxis_title="Orders")
        st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        restaurant_summary.sort_values("gross_revenue", ascending=False),
        use_container_width=True
    )


with tab_items:
    st.markdown("### Item Performance")

    if item_perf.empty:
        st.info("No item performance data for the selected filters.")
    else:
        item_summary = (
            item_perf.groupby(["item_category", "item_name"], as_index=False)
            .agg(
                item_total_revenue=("item_total_revenue", "sum"),
                item_base_revenue=("item_base_revenue", "sum"),
                item_option_revenue=("item_option_revenue", "sum"),
                total_quantity_sold=("total_quantity_sold", "sum"),
                distinct_orders=("distinct_orders", "sum"),
                distinct_customers=("distinct_customers", "sum"),
                avg_item_price=("avg_item_price", "mean")
            )
        )

        c1, c2 = st.columns(2)

        with c1:
            top_items = item_summary.sort_values("item_total_revenue", ascending=False).head(20)
            fig = px.bar(
                top_items,
                x="item_name",
                y="item_total_revenue",
                color="item_category",
                title="Top Items by Revenue"
            )
            fig.update_layout(xaxis_title="Item", yaxis_title="Revenue")
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            top_quantity = item_summary.sort_values("total_quantity_sold", ascending=False).head(20)
            fig = px.bar(
                top_quantity,
                x="item_name",
                y="total_quantity_sold",
                color="item_category",
                title="Top Items by Quantity Sold"
            )
            fig.update_layout(xaxis_title="Item", yaxis_title="Quantity Sold")
            st.plotly_chart(fig, use_container_width=True)

        category_summary = (
            item_summary.groupby("item_category", as_index=False)
            .agg(
                item_total_revenue=("item_total_revenue", "sum"),
                total_quantity_sold=("total_quantity_sold", "sum")
            )
            .sort_values("item_total_revenue", ascending=False)
        )

        fig = px.bar(
            category_summary,
            x="item_category",
            y="item_total_revenue",
            title="Revenue by Item Category"
        )
        fig.update_layout(xaxis_title="Category", yaxis_title="Revenue")
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            item_summary.sort_values("item_total_revenue", ascending=False),
            use_container_width=True
        )


with tab_loyalty:
    st.markdown("### Loyalty Analysis")

    if loyalty.empty:
        st.info("No loyalty data for the selected filters.")
    else:
        loyalty_summary = (
            loyalty.groupby("loyalty_segment", as_index=False)
            .agg(
                gross_revenue=("gross_revenue", "sum"),
                total_orders=("total_orders", "sum"),
                distinct_customers=("distinct_customers", "sum"),
                total_item_quantity=("total_item_quantity", "sum"),
                option_revenue=("option_revenue", "sum")
            )
        )
        loyalty_summary["average_order_value"] = (
            loyalty_summary["gross_revenue"] /
            loyalty_summary["total_orders"].replace(0, pd.NA)
        ).fillna(0)

        c1, c2 = st.columns(2)

        with c1:
            fig = px.bar(
                loyalty_summary,
                x="loyalty_segment",
                y="gross_revenue",
                title="Revenue by Loyalty Segment"
            )
            fig.update_layout(xaxis_title="Loyalty Segment", yaxis_title="Revenue")
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            fig = px.bar(
                loyalty_summary,
                x="loyalty_segment",
                y="average_order_value",
                title="Average Order Value by Loyalty Segment"
            )
            fig.update_layout(xaxis_title="Loyalty Segment", yaxis_title="AOV")
            st.plotly_chart(fig, use_container_width=True)

        loyalty_trend = (
            loyalty.groupby(["order_date", "loyalty_segment"], as_index=False)
            .agg(gross_revenue=("gross_revenue", "sum"))
        )

        fig = px.line(
            loyalty_trend,
            x="order_date",
            y="gross_revenue",
            color="loyalty_segment",
            title="Loyalty vs Non-Loyalty Revenue Trend",
            markers=True
        )
        fig.update_layout(xaxis_title="Order Date", yaxis_title="Revenue")
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(loyalty_summary, use_container_width=True)


with tab_orders:
    st.markdown("### Order Drilldown")

    if orders.empty:
        st.info("No order records for the selected filters.")
    else:
        st.dataframe(
            orders.sort_values("order_date", ascending=False),
            use_container_width=True
        )

        csv = orders.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download visible orders as CSV",
            data=csv,
            file_name="global_partner_order_drilldown.csv",
            mime="text/csv"
        )


st.markdown("---")
st.caption(
    "Data source: Athena Gold tables over S3. Currency filter removed. Restaurant visuals use restaurant_name alias."
)
