#!/usr/bin/env python3
"""
Meridian Bank K-Means Fraud Cluster Explorer — EDB Postgres AI Branded
Light Theme Version (Aligned with PGAA Dashboard)
Run: python3 dashboard.py
Access: http://localhost:5003
"""
from typing import Optional
import os
import textwrap
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc

# ─────────────────────────────────────────────
# CONNECTION CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("WPGHOST",   "localhost"),
    "port":     int(os.getenv("WPGPORT",   "5432")),
    "dbname":   os.getenv("WPGDB",     "demo"),
    "user":     os.getenv("WPGUSER",   "gpadmin"),
    "password": os.getenv("WPGPASS",   ""),
}

CLUSTER_LABELS = {
    0: "Normal activity",
    1: "Card testing",
    2: "Bust-out",
    3: "Structuring",
    4: "Velocity abuse",
}

# EDB Corporate Palette (Light Theme)
COLORS = ["#3DBFBF", "#1D9E75", "#D85A30", "#E8972A", "#D94040"]

# ─────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_cluster_points() -> pd.DataFrame:
    sql = textwrap.dedent("""
        WITH fraud_accounts AS (
            -- Include ALL fraud accounts (card-testing, bust-out, structuring)
            SELECT
                a.account_id,
                a.cluster_id,
                a.inferred_label,
                f.txn_count, f.distinct_mccs, f.distinct_merchants,
                f.total_amount, f.avg_amount, f.merchant_concentration,
                f.stddev_amount, f.amount_cv
            FROM bfsi_demo.kmeans_labeled a
            JOIN bfsi_demo.account_features f USING (account_id)
            WHERE a.inferred_label IN ('CARD-TESTING', 'BUST-OUT', 'STRUCTURING')
        ),
        normal_sample AS (
            -- Sample 2000 normal accounts
            SELECT
                a.account_id,
                a.cluster_id,
                a.inferred_label,
                f.txn_count, f.distinct_mccs, f.distinct_merchants,
                f.total_amount, f.avg_amount, f.merchant_concentration,
                f.stddev_amount, f.amount_cv
            FROM bfsi_demo.kmeans_labeled a
            JOIN bfsi_demo.account_features f USING (account_id)
            WHERE a.inferred_label = 'NORMAL'
            ORDER BY RANDOM()
            LIMIT 2000
        )
        SELECT
            account_id,
            cluster_id,
            inferred_label AS label,
            txn_count AS txns,
            distinct_mccs AS mccs,
            distinct_merchants AS merchants,
            ROUND(total_amount::numeric, 2) AS spend,
            ROUND(avg_amount::numeric, 2) AS avg_ticket,
            ROUND(merchant_concentration::numeric, 4) AS merch_concentration,
            ROUND(stddev_amount::numeric, 2) AS stddev_amount,
            ROUND(amount_cv::numeric, 4) AS amount_cv
        FROM fraud_accounts
        UNION ALL
        SELECT
            account_id,
            cluster_id,
            inferred_label,
            txn_count,
            distinct_mccs,
            distinct_merchants,
            ROUND(total_amount::numeric, 2),
            ROUND(avg_amount::numeric, 2),
            ROUND(merchant_concentration::numeric, 4),
            ROUND(stddev_amount::numeric, 2),
            ROUND(amount_cv::numeric, 4)
        FROM normal_sample
        ORDER BY label, cluster_id, spend DESC
    """)
    with get_connection() as conn:
        return pd.read_sql(sql, conn)

def load_cluster_summary() -> pd.DataFrame:
    sql = textwrap.dedent("""
        SELECT
            a.cluster_id,
            a.inferred_label AS persona,
            COUNT(*)                                       AS acct_count,
            ROUND(AVG(f.txn_count)::numeric, 1)           AS avg_txns,
            ROUND(AVG(f.distinct_mccs)::numeric, 1)       AS avg_mccs,
            ROUND(AVG(f.distinct_merchants)::numeric, 1)  AS avg_merchants,
            ROUND(AVG(f.total_amount)::numeric, 2)        AS avg_spend,
            ROUND(AVG(f.merchant_concentration)::numeric, 4) AS avg_concentration,
            ROUND(AVG(f.stddev_amount)::numeric, 2)       AS avg_stddev,
            ROUND(AVG(f.amount_cv)::numeric, 4)           AS avg_cv
        FROM bfsi_demo.kmeans_labeled a
        JOIN bfsi_demo.account_features   f USING (account_id)
        GROUP BY a.cluster_id, a.inferred_label
        ORDER BY
            CASE a.inferred_label
                WHEN 'CARD-TESTING' THEN 1
                WHEN 'BUST-OUT' THEN 2
                WHEN 'STRUCTURING' THEN 3
                WHEN 'NORMAL' THEN 4
            END, acct_count DESC
    """)
    with get_connection() as conn:
        return pd.read_sql(sql, conn)

# ─────────────────────────────────────────────
# STYLE & LAYOUT
# ─────────────────────────────────────────────

def _layout(height=360):
    return dict(
        height=height,
        margin=dict(l=40, r=20, t=20, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="'IBM Plex Sans', sans-serif", color="#3D3D3D", size=11),
        legend=dict(bgcolor="rgba(255,255,255,0.8)", font=dict(size=10)),
        xaxis=dict(gridcolor="#EEEEEE", zerolinecolor="#E5E5E5"),
        yaxis=dict(gridcolor="#EEEEEE", zerolinecolor="#E5E5E5"),
    )

# ─────────────────────────────────────────────
# DASH APP
# ─────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono&display=swap"
    ],
    title="Meridian Bank — Fraud Cluster Explorer",
)

AXIS_OPTIONS = [
    {"label": "Txn count",      "value": "txns"},
    {"label": "Distinct MCCs",  "value": "mccs"},
    {"label": "Distinct merchants", "value": "merchants"},
    {"label": "Total spend",    "value": "spend"},
    {"label": "Avg ticket",     "value": "avg_ticket"},
    {"label": "Merchant concentration", "value": "merch_concentration"},
    {"label": "Stddev amount",  "value": "stddev_amount"},
    {"label": "Amount CV (variance)", "value": "amount_cv"},
]

# Light Theme Header
header = html.Nav(className="nav", style={
    "background": "#fff", "borderBottom": "1px solid #E2E2E2",
    "borderTop": "3px solid #3DBFBF", "height": "58px",
    "display": "flex", "alignItems": "center", "padding": "0 28px",
    "position": "sticky", "top": "0", "zIndex": "100", "boxShadow": "0 1px 3px rgba(0,0,0,.06)"
}, children=[
    html.A(style={"textDecoration": "none", "marginRight": "18px", "display": "flex", "alignItems": "baseline", "gap": "4px"}, children=[
        html.Span("EDB", style={"fontSize": "17px", "fontWeight": "800", "letterSpacing": "1px", "color": "#27A67A"}),
        html.Span("WHPG", style={"fontSize": "17px", "fontWeight": "700", "letterSpacing": ".5px", "color": "#27A67A"}),
    ]),
    html.Div(style={"width": "1px", "height": "24px", "background": "#E2E2E2", "margin": "0 16px"}),
    html.Span("K-Means Cluster Explorer", style={"fontSize": "13px", "fontWeight": "500", "color": "#555555"}),
    html.Div(style={"flex": "1"}),
    html.Div(id="conn-status", style={"fontSize": "12px", "color": "#555555"})
])

app.layout = html.Div(style={"background": "#F5F5F5", "minHeight": "100vh"}, children=[
    header,
    dbc.Container(fluid=True, style={"padding": "28px"}, children=[
        
        # metric cards
        dbc.Row(id="metric-cards", style={"marginBottom": "26px"}),

        # main charts row
        dbc.Row([
            dbc.Col([
                dbc.Card(style={"borderRadius": "12px", "border": "1px solid #E2E2E2", "boxShadow": "0 1px 3px rgba(0,0,0,.06)"}, children=[
                    dbc.CardHeader([
                        html.Span("Scatter: ", style={"fontSize": "11px", "fontWeight": "600", "textTransform": "uppercase"}),
                        dcc.Dropdown(id="x-axis", options=AXIS_OPTIONS, value="txns", clearable=False,
                                     style={"width": "150px", "display": "inline-block", "fontSize": "12px"}),
                        html.Span(" vs ", style={"margin": "0 10px"}),
                        dcc.Dropdown(id="y-axis", options=AXIS_OPTIONS, value="merchants", clearable=False,
                                     style={"width": "150px", "display": "inline-block", "fontSize": "12px"}),
                    ], style={"background": "#FAFAFA", "borderBottom": "1px solid #E2E2E2", "padding": "10px 20px"}),
                    dbc.CardBody(dcc.Graph(id="scatter-plot", config={"displayModeBar": False}))
                ])
            ], md=8),
            dbc.Col([
                dbc.Card(style={"borderRadius": "12px", "border": "1px solid #E2E2E2", "marginBottom": "20px"}, children=[
                    dbc.CardHeader("Cluster Sizes", style={"fontSize": "11px", "fontWeight": "600", "textTransform": "uppercase"}),
                    dbc.CardBody(dcc.Graph(id="dist-plot", config={"displayModeBar": False}, style={"height": "250px"}))
                ]),
                dbc.Card(style={"borderRadius": "12px", "border": "1px solid #E2E2E2"}, children=[
                    dbc.CardHeader("Radar Profiles", style={"fontSize": "11px", "fontWeight": "600", "textTransform": "uppercase"}),
                    dbc.CardBody(dcc.Graph(id="radar-plot", config={"displayModeBar": False}, style={"height": "300px"}))
                ])
            ], md=4),
        ], style={"marginBottom": "26px"}),

        # heatmap
        dbc.Row([
            dbc.Col([
                dbc.Card(style={"borderRadius": "12px", "border": "1px solid #E2E2E2"}, children=[
                    dbc.CardHeader("Centroid Heatmap (Z-Scored)", style={"fontSize": "11px", "fontWeight": "600", "textTransform": "uppercase"}),
                    dbc.CardBody(dcc.Graph(id="heatmap-plot", config={"displayModeBar": False}))
                ])
            ], width=12),
        ], style={"marginBottom": "26px"}),

        # drilldown section
        dbc.Row([
            dbc.Col([
                dbc.Card(style={"borderRadius": "12px", "border": "1px solid #E2E2E2"}, children=[
                    dbc.CardHeader([
                        html.Span("Cluster Drilldown", style={"fontSize": "11px", "fontWeight": "600", "textTransform": "uppercase"}),
                        dcc.Dropdown(id="drilldown-cluster", 
                                     options=[{"label": f"C{i} — {CLUSTER_LABELS[i]}", "value": i} for i in range(5)],
                                     value=0, clearable=False,
                                     style={"width": "300px", "marginLeft": "20px", "display": "inline-block", "fontSize": "12px"}),
                    ], style={"background": "#FAFAFA", "padding": "10px 20px"}),
                    dbc.CardBody([
                        dbc.Tabs([
                            dbc.Tab(label="Top IPs", tab_id="tab-ips"),
                        ], id="drilldown-tabs", active_tab="tab-ips"),
                        html.Div(id="drilldown-content", style={"marginTop": "15px"})
                    ])
                ])
            ], width=12),
        ]),
    ]),
    dcc.Store(id="store-points"),
    dcc.Store(id="store-summary"),
    # Add reload button at bottom or corner
    html.Div(dbc.Button("⟳ Reload", id="btn-reload", color="info", size="sm"), 
             style={"position": "fixed", "bottom": "20px", "right": "20px"})
])

# ─────────────────────────────────────────────
# CALLBACKS (Modified for styling)
# ─────────────────────────────────────────────

@app.callback(
    Output("store-points",  "data"),
    Output("store-summary", "data"),
    Output("conn-status",   "children"),
    Input("btn-reload", "n_clicks"),
)
def load_data(_):
    try:
        pts  = load_cluster_points()
        summ = load_cluster_summary()
        msg = html.Div([
            html.Span("● ", style={"color": "#27A67A"}),
            html.Span(f"Connected: {len(pts):,} accounts loaded")
        ])
        return pts.to_json(date_format="iso", orient="split"), summ.to_json(date_format="iso", orient="split"), msg
    except Exception as exc:
        return None, None, html.Span(f"● Error: {exc}", style={"color": "#D94040"})

@app.callback(
    Output("metric-cards", "children"),
    Input("store-summary", "data"),
)
def update_metrics(summ_json):
    if not summ_json: return []
    summ = pd.read_json(summ_json, orient="split")
    
    def card(label, value):
        return dbc.Col(html.Div(style={
            "background": "#fff", "border": "1px solid #E2E2E2", "borderTop": "3px solid #3DBFBF",
            "borderRadius": "12px", "padding": "15px 17px", "boxShadow": "0 1px 3px rgba(0,0,0,.06)"
        }, children=[
            html.Div(label, style={"fontSize": "10.5px", "fontWeight": "600", "textTransform": "uppercase", "color": "#999", "letterSpacing": ".7px"}),
            html.Div(value, style={"fontSize": "21px", "fontWeight": "700", "fontFamily": "IBM Plex Mono", "color": "#222"})
        ]))

    return [
        card("Total Accounts", f"{int(summ['acct_count'].sum()):,}"),
        card("Avg Txns", f"{summ['avg_txns'].mean():.1f}"),
        card("Max Merchant Concentration", f"{summ['avg_concentration'].max():.3f}"),
        card("Largest Cluster", f"C{int(summ.loc[summ['acct_count'].idxmax(), 'cluster_id'])}")
    ]

@app.callback(
    Output("scatter-plot", "figure"),
    Output("dist-plot",    "figure"),
    Output("radar-plot",   "figure"),
    Output("heatmap-plot", "figure"),
    Input("store-points",  "data"),
    Input("store-summary", "data"),
    Input("x-axis", "value"),
    Input("y-axis", "value"),
)
def update_charts(pts_json, summ_json, x_col, y_col):
    if not pts_json or not summ_json: return [go.Figure()]*4
    pts = pd.read_json(pts_json, orient="split")
    summ = pd.read_json(summ_json, orient="split")
    
    # Scatter - use label from SQL (inferred_label), with explicit color mapping
    color_map = {
        "CARD-TESTING": "#3DBFBF",  # teal
        "BUST-OUT": "#D85A30",       # orange
        "STRUCTURING": "#1D9E75",    # green
        "NORMAL": "#E8972A",         # amber
    }
    fig_s = px.scatter(pts, x=x_col, y=y_col, color="label",
                       color_discrete_map=color_map, opacity=0.6,
                       category_orders={"label": ["CARD-TESTING", "BUST-OUT", "STRUCTURING", "NORMAL"]})
    fig_s.update_layout(**_layout(height=450))

    # Distribution - use persona from SQL
    fig_d = px.bar(summ, x="acct_count", y="persona", orientation="h", color="persona",
                   color_discrete_map=color_map,
                   category_orders={"persona": ["CARD-TESTING", "BUST-OUT", "STRUCTURING", "NORMAL"]})
    fig_d.update_layout(**_layout(height=250), showlegend=False)

    # Radar
    fig_r = go.Figure()
    dims = ["avg_txns", "avg_mccs", "avg_merchants", "avg_spend", "avg_concentration", "avg_cv"]
    for i, row in summ.iterrows():
        # normalize for radar
        norm_vals = [row[d]/summ[d].max() if summ[d].max() > 0 else 0 for d in dims]
        fig_r.add_trace(go.Scatterpolar(r=norm_vals + [norm_vals[0]],
                                       theta=["Txns", "MCCs", "Merchants", "Spend", "Concentration", "CV", "Txns"],
                                       fill='toself', name=f"C{int(row['cluster_id'])}", line_color=COLORS[i%5]))
    fig_r.update_layout(**_layout(height=300))

    # Heatmap
    z = summ[dims].values.astype(float)
    fig_h = px.imshow(z, x=["Txns", "MCCs", "Merchants", "Spend", "Concentration", "CV"],
                      y=[f"C{int(i)}" for i in summ['cluster_id']], color_continuous_scale="RdBu_r")
    fig_h.update_layout(**_layout(height=300))

    return fig_s, fig_d, fig_r, fig_h

@app.callback(
    Output("drilldown-content", "children"),
    Input("drilldown-cluster", "value"),
    Input("store-points",      "data"),
)
def update_drilldown(cluster_id, pts_json):
    if pts_json is None: return html.P("No data.")
    pts = pd.read_json(pts_json, orient="split")
    df = pts[pts["cluster_id"] == cluster_id].head(20)
    
    return dash_table.DataTable(
        data=df.to_dict("records"),
        columns=[{"name": i, "id": i} for i in df.columns if i != 'label'],
        style_header={'backgroundColor': '#FAFAFA', 'fontWeight': 'bold', 'color': '#555'},
        style_cell={'backgroundColor': '#FFF', 'color': '#333', 'fontFamily': 'IBM Plex Mono', 'fontSize': '12px'},
        style_table={'overflowX': 'auto'}
    )


if __name__ == '__main__':
    print('\n  EDB MADlib Kmeans Dashboard: http://localhost:5003\n')
    app.run(host='0.0.0.0', port=5003, debug=True, threaded=True)