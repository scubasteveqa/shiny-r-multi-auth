"""
Shiny for Python - Multi-Source Analytics Dashboard
====================================================
Uses content.associations.find_by() to discover integration GUIDs
by type/name, then passes them as audience to get_credentials().

Based on: https://docs.posit.co/connect/user/oauth-integrations/
  #obtaining-credentials-within-content-associated-with-multiple-integrations
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from databricks import sql as dbsql
from posit import connect
from posit.connect.oauth import types
from shiny import App, Inputs, Outputs, Session, reactive, render, ui
from shinywidgets import output_widget, render_widget


# ── Data helpers ────────────────────────────────────────────────────────────

def fetch_snowflake(access_token: str) -> pd.DataFrame:
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        token=access_token,
        authenticator="oauth",
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    try:
        df = pd.read_sql("SELECT * FROM SALES", conn)
    finally:
        conn.close()
    df.columns = df.columns.str.upper()
    df["SALE_DATE"] = pd.to_datetime(df["SALE_DATE"])
    df["MONTH"] = df["SALE_DATE"].dt.to_period("M").astype(str)
    return df


def fetch_databricks(access_token: str) -> pd.DataFrame:
    conn = dbsql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=access_token,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    t.dateTime,
                    t.product,
                    t.quantity,
                    t.totalPrice,
                    c.continent,
                    c.country,
                    f.name AS franchise_name
                FROM samples.bakehouse.sales_transactions t
                JOIN samples.bakehouse.sales_customers c
                    ON t.customerID = c.customerID
                JOIN samples.bakehouse.sales_franchises f
                    ON t.franchiseID = f.franchiseID
            """)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    finally:
        conn.close()
    df = pd.DataFrame(rows, columns=cols)
    df.columns = df.columns.str.lower()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["month"] = df["datetime"].dt.to_period("M").astype(str)
    return df


# ── UI ──────────────────────────────────────────────────────────────────────

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.tags.h3("Multi-Source Dashboard"),
        ui.tags.p(
            "Snowflake + Databricks via OAuth",
            style="color: #6c757d; font-size: 0.85rem; margin: 0 0 1rem 0;",
        ),
        ui.input_action_button("load_data", "Refresh All Data", class_="btn-primary w-100"),
        ui.tags.script(
            "setTimeout(function() { document.getElementById('load_data').click(); }, 500);"
        ),
        ui.tags.hr(),
        ui.tags.label("Snowflake Filters", style="font-weight: 600;"),
        ui.input_select("sf_category", "Category", choices=["All"], selected="All"),
        ui.input_select("sf_region", "Region", choices=["All"], selected="All"),
        ui.tags.hr(),
        ui.tags.label("Databricks Filters", style="font-weight: 600;"),
        ui.input_select("db_continent", "Continent", choices=["All"], selected="All"),
        ui.input_select("db_franchise", "Franchise", choices=["All"], selected="All"),
        width=260,
    ),

    # Snowflake section
    ui.tags.h4("Snowflake - Sales", style="margin-top: 0.5rem;"),
    ui.layout_columns(
        ui.value_box("SF Total Sales", ui.output_text("sf_total_sales"), theme="primary"),
        ui.value_box("SF Orders", ui.output_text("sf_total_orders"), theme="info"),
        ui.value_box("SF Avg Order", ui.output_text("sf_avg_order"), theme="success"),
        col_widths=[4, 4, 4],
    ),
    ui.layout_columns(
        ui.card(ui.card_header("Sales by Category"), output_widget("sf_chart_category")),
        ui.card(ui.card_header("Sales by Region"), output_widget("sf_chart_region")),
        col_widths=[6, 6],
    ),

    # Databricks section
    ui.tags.h4("Databricks - Bakehouse", style="margin-top: 1.5rem;"),
    ui.layout_columns(
        ui.value_box("DB Revenue", ui.output_text("db_total_revenue"), theme="primary"),
        ui.value_box("DB Orders", ui.output_text("db_total_orders"), theme="info"),
        ui.value_box("DB Franchises", ui.output_text("db_franchise_count"), theme="warning"),
        col_widths=[4, 4, 4],
    ),
    ui.layout_columns(
        ui.card(ui.card_header("Revenue by Franchise"), output_widget("db_chart_franchise")),
        ui.card(ui.card_header("Revenue by Continent"), output_widget("db_chart_continent")),
        col_widths=[6, 6],
    ),

    # Combined trend
    ui.layout_columns(
        ui.card(
            ui.card_header("Monthly Trends - Both Sources"),
            output_widget("combined_trend"),
        ),
        col_widths=[12],
    ),

    title="Multi-Source Analytics",
)


# ── Server ──────────────────────────────────────────────────────────────────

def server(i: Inputs, o: Outputs, session: Session):
    sf_data = reactive.Value(None)
    db_data = reactive.Value(None)

    @reactive.effect
    @reactive.event(i.load_data)
    def load_all():
        session_token = session.http_conn.headers.get(
            "Posit-Connect-User-Session-Token"
        )
        if not session_token:
            ui.notification_show(
                "No session token found. Deploy this app on Posit Connect.",
                type="error",
            )
            return

        client = connect.Client()
        current_content = client.content.get()

        # Discover integrations via content's OAuth associations
        sf_assoc = current_content.oauth.associations.find_by(
            integration_type=types.OAuthIntegrationType.SNOWFLAKE
        )
        db_assoc = current_content.oauth.associations.find_by(
            integration_type=types.OAuthIntegrationType.DATABRICKS
        )

        if sf_assoc is None or db_assoc is None:
            ui.notification_show(
                f"Missing association: snowflake={sf_assoc is not None}, databricks={db_assoc is not None}",
                type="error",
            )
            return

        sf_guid = sf_assoc.get("oauth_integration_guid")
        db_guid = db_assoc.get("oauth_integration_guid")

        # Fetch Snowflake
        try:
            sf_creds = client.oauth.get_credentials(session_token, audience=sf_guid)
            df_sf = fetch_snowflake(sf_creds["access_token"])
            sf_data.set(df_sf)

            categories = ["All"] + sorted(df_sf["CATEGORY"].dropna().unique().tolist())
            regions = ["All"] + sorted(df_sf["REGION"].dropna().unique().tolist())
            ui.update_select("sf_category", choices=categories, selected="All")
            ui.update_select("sf_region", choices=regions, selected="All")

            ui.notification_show(f"Snowflake: {len(df_sf)} rows", type="message")
        except Exception as e:
            ui.notification_show(f"Snowflake error: {e}", type="error", duration=10)

        # Fetch Databricks
        try:
            db_creds = client.oauth.get_credentials(session_token, audience=db_guid)
            df_db = fetch_databricks(db_creds["access_token"])
            db_data.set(df_db)

            continents = ["All"] + sorted(df_db["continent"].dropna().unique().tolist())
            franchises = ["All"] + sorted(df_db["franchise_name"].dropna().unique().tolist())
            ui.update_select("db_continent", choices=continents, selected="All")
            ui.update_select("db_franchise", choices=franchises, selected="All")

            ui.notification_show(f"Databricks: {len(df_db)} rows", type="message")
        except Exception as e:
            ui.notification_show(f"Databricks error: {e}", type="error", duration=10)

    # ── Snowflake filtered data ──

    @reactive.calc
    def sf_filtered():
        df = sf_data()
        if df is None:
            return None
        if i.sf_category() != "All":
            df = df[df["CATEGORY"] == i.sf_category()]
        if i.sf_region() != "All":
            df = df[df["REGION"] == i.sf_region()]
        return df

    @render.text
    def sf_total_sales():
        df = sf_filtered()
        return f"${df['TOTAL_AMOUNT'].sum():,.2f}" if df is not None else "--"

    @render.text
    def sf_total_orders():
        df = sf_filtered()
        return f"{len(df):,}" if df is not None else "--"

    @render.text
    def sf_avg_order():
        df = sf_filtered()
        if df is None or len(df) == 0:
            return "--"
        return f"${df['TOTAL_AMOUNT'].mean():,.2f}"

    @render_widget
    def sf_chart_category():
        df = sf_filtered()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=350)
            return fig
        agg = df.groupby("CATEGORY", as_index=False)["TOTAL_AMOUNT"].sum()
        return px.bar(
            agg, x="CATEGORY", y="TOTAL_AMOUNT",
            color="CATEGORY",
            labels={"TOTAL_AMOUNT": "Sales ($)", "CATEGORY": "Category"},
        )

    @render_widget
    def sf_chart_region():
        df = sf_filtered()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=350)
            return fig
        agg = df.groupby("REGION", as_index=False)["TOTAL_AMOUNT"].sum()
        return px.pie(
            agg, names="REGION", values="TOTAL_AMOUNT",
            labels={"TOTAL_AMOUNT": "Sales ($)", "REGION": "Region"},
        )

    # ── Databricks filtered data ──

    @reactive.calc
    def db_filtered():
        df = db_data()
        if df is None:
            return None
        if i.db_continent() != "All":
            df = df[df["continent"] == i.db_continent()]
        if i.db_franchise() != "All":
            df = df[df["franchise_name"] == i.db_franchise()]
        return df

    @render.text
    def db_total_revenue():
        df = db_filtered()
        return f"${df['totalprice'].sum():,.2f}" if df is not None else "--"

    @render.text
    def db_total_orders():
        df = db_filtered()
        return f"{len(df):,}" if df is not None else "--"

    @render.text
    def db_franchise_count():
        df = db_filtered()
        return str(df["franchise_name"].nunique()) if df is not None else "--"

    @render_widget
    def db_chart_franchise():
        df = db_filtered()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=350)
            return fig
        agg = (
            df.groupby("franchise_name", as_index=False)["totalprice"]
            .sum()
            .sort_values("totalprice", ascending=True)
        )
        return px.bar(
            agg, x="totalprice", y="franchise_name", orientation="h",
            labels={"totalprice": "Revenue ($)", "franchise_name": "Franchise"},
        )

    @render_widget
    def db_chart_continent():
        df = db_filtered()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=350)
            return fig
        agg = df.groupby("continent", as_index=False)["totalprice"].sum()
        return px.pie(
            agg, names="continent", values="totalprice",
            labels={"totalprice": "Revenue ($)", "continent": "Continent"},
        )

    # ── Combined trend ──

    @render_widget
    def combined_trend():
        df_sf = sf_filtered()
        df_db = db_filtered()
        if df_sf is None and df_db is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=400)
            return fig

        fig = go.Figure()
        if df_sf is not None:
            agg_sf = df_sf.groupby("MONTH", as_index=False)["TOTAL_AMOUNT"].sum()
            fig.add_trace(go.Scatter(
                x=agg_sf["MONTH"], y=agg_sf["TOTAL_AMOUNT"],
                mode="lines+markers", name="Snowflake Sales",
            ))
        if df_db is not None:
            agg_db = df_db.groupby("month", as_index=False)["totalprice"].sum()
            fig.add_trace(go.Scatter(
                x=agg_db["month"], y=agg_db["totalprice"],
                mode="lines+markers", name="Databricks Revenue",
            ))
        fig.update_layout(
            xaxis_title="Month", yaxis_title="Amount ($)",
            template="plotly_white", height=400,
        )
        return fig


app = App(app_ui, server)
