#!/usr/bin/env python3
"""
Vector DB Benchmark Dashboard v2
Modern look and feel inspired by Redis benchmark charts.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any

import dash
from dash import dcc, html, callback, Input, Output, State
import plotly.graph_objects as go
import pandas as pd

# Color palette matching their scheme - maps engine names to colors
ENGINE_COLORS = {
    "redis": "#DC244C",  # Redis RED
    "mongodb": "#00684A",  # MongoDB green
    "milvus": "#1493cc",
    "weaviate": "#01cc26",
    "qdrant": "#bc1439",
    "elastic": "#f9b110",
}

# Fallback colors for unknown engines
FALLBACK_COLORS = [
    "#DC244C", "#4A90D9", "#50C878", "#9B59B6",
    "#F39C12", "#1ABC9C", "#E74C3C", "#3498DB",
]

# Which metrics are "lower is better"
LOWER_IS_BETTER = {
    "rps": False,
    "mean_time": True,
    "p95_time": True,
    "p99_time": True,
    "upload_time": True,
    "total_time": True,
}


def get_engine_color(engine: str, index: int) -> str:
    """Get color for an engine, with fallback."""
    # Direct match on engine name
    if engine in ENGINE_COLORS:
        return ENGINE_COLORS[engine]
    # Fallback
    return FALLBACK_COLORS[index % len(FALLBACK_COLORS)]


def filter_pareto_points(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """
    Filter to keep only Pareto-optimal points.

    A point is Pareto-optimal if no other point has both:
    - Higher precision AND better metric value

    This creates a smooth frontier curve.
    """
    if df.empty:
        return df

    lower_is_better = LOWER_IS_BETTER.get(metric, False)

    # Sort by precision descending (highest first)
    df = df.sort_values("mean_precisions", ascending=False).copy()

    pareto_points = []
    best_value = None

    for _, row in df.iterrows():
        current_value = row[metric]

        if best_value is None:
            # First point (highest precision) is always included
            pareto_points.append(row)
            best_value = current_value
        else:
            # Check if this point improves the metric
            if lower_is_better:
                is_better = current_value < best_value
            else:
                is_better = current_value > best_value

            if is_better:
                pareto_points.append(row)
                best_value = current_value

    return pd.DataFrame(pareto_points)


def load_results(results_dir: str = "results/") -> pd.DataFrame:
    """Load all benchmark results from JSON files into a DataFrame."""
    records = []

    for filepath in Path(results_dir).glob("*.json"):
        filename = filepath.name
        if "summary" in filename:
            continue

        with open(filepath, "r") as f:
            data = json.load(f)

        params = data.get("params", {})
        results = data.get("results", {})

        record = {
            "filename": filename,
            "dataset": params.get("dataset", ""),
            "experiment": params.get("experiment", ""),
            "engine": params.get("engine", ""),
        }

        if "search" in filename:
            record["type"] = "search"
            record["parallel"] = params.get("parallel", 1)
            search_params = params.get("search_params", {})
            ef_runtime = search_params.get("ef", 0) or search_params.get("numCandidates", 0)
            record["search_ef"] = ef_runtime
            record["rps"] = results.get("rps", 0)
            record["mean_time"] = results.get("mean_time", 0) * 1000
            record["p95_time"] = results.get("p95_time", 0) * 1000
            record["p99_time"] = results.get("p99_time", 0) * 1000
            record["mean_precisions"] = results.get("mean_precisions", 0)
        elif "upload" in filename:
            record["type"] = "upload"
            record["parallel"] = params.get("parallel", 1)
            record["upload_time"] = results.get("upload_time", 0)
            record["total_time"] = results.get("total_time", 0)
            memory_usage = results.get("memory_usage", {})
            used_memory_bytes = memory_usage.get("used_memory", [0])
            used_memory_bytes = used_memory_bytes[0] if used_memory_bytes else 0
            index_info = memory_usage.get("index_info", {})
            index_memory_mb = float(index_info.get("total_index_memory_sz_mb", 0) or 0)
            index_memory_bytes = index_memory_mb * 1024 * 1024
            data_memory_bytes = max(0, used_memory_bytes - index_memory_bytes)
            record["total_memory_gb"] = used_memory_bytes / (1024 ** 3)
            record["index_memory_gb"] = index_memory_bytes / (1024 ** 3)
            record["data_memory_gb"] = data_memory_bytes / (1024 ** 3)
        else:
            continue

        records.append(record)

    return pd.DataFrame(records)


# Load data
df = load_results()
datasets = sorted([v for v in df["dataset"].unique() if v])
parallel_values = sorted(df[df["type"] == "search"]["parallel"].unique(), reverse=True)
experiments = sorted([v for v in df["experiment"].unique() if v])

# Create Dash app with custom CSS
app = dash.Dash(__name__, title="Vector DB Benchmark")

# Custom CSS styles
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            body { background: #f8f9fc; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; }
            .page-header { max-width: 960px; margin: 40px auto 0; padding: 0 24px; }
            .page-header h1 { font-size: 1.75rem; font-weight: 700; color: #1a1a2e; margin-bottom: 4px; }
            .page-header p { color: #6c757d; font-size: 0.95rem; margin: 0; }
            .benchmark-wrapper { max-width: 960px; margin: 24px auto 48px; background: #fff; border: 1px solid #e2e6ef; border-radius: 12px; padding: 28px 32px 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }
            .controls { display: flex; flex-wrap: wrap; align-items: center; gap: 16px; margin-bottom: 20px; padding-bottom: 16px; border-bottom: 1px solid #eef0f5; }
            .control-group { display: flex; align-items: center; gap: 6px; }
            .control-group label { font-size: 0.82rem; font-weight: 600; color: #4a5568; text-transform: uppercase; letter-spacing: 0.03em; }
            .Select-control { min-width: 180px; }
            .radio-group { display: flex; align-items: center; gap: 4px; background: #f1f3f8; border-radius: 8px; padding: 4px; }
            .radio-item { font-size: 0.82rem; padding: 5px 12px; border-radius: 6px; cursor: pointer; color: #4a5568; transition: all 0.15s ease; white-space: nowrap; }
            .radio-item.selected { background: #fff; color: #1a1a2e; font-weight: 600; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
            .legend-hint { display: flex; gap: 24px; margin-bottom: 12px; flex-wrap: wrap; }
            .legend-item { display: flex; align-items: center; gap: 6px; font-size: 0.85rem; color: #4a5568; }
            .legend-dot { width: 12px; height: 12px; border-radius: 50%; display: inline-block; }
            .precision-slider { margin: 0 5% 20px; }
            .slider-label { font-size: 0.78rem; color: #9ca3af; text-align: center; display: block; margin-top: 4px; }
            .data-table { width: 100%; border-collapse: collapse; font-size: 0.85rem; margin-top: 16px; }
            .data-table thead tr { background: #f1f3f8; }
            .data-table th, .data-table td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #eef0f5; }
            .data-table th { font-weight: 600; color: #4a5568; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.03em; }
            .data-table tbody tr:hover { background: #f8f9fc; }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Layout
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("Vector DB Benchmark"),
        html.P("Compare vector search performance across different engines and configurations"),
    ], className="page-header"),

    # Main wrapper
    html.Div([
        # Controls row
        html.Div([
            # Dataset selector
            html.Div([
                html.Label("Dataset"),
                dcc.Dropdown(
                    id="dataset-selector",
                    options=[{"label": d, "value": d} for d in datasets],
                    value=datasets[0] if datasets else None,
                    clearable=False,
                    style={"minWidth": "200px"},
                ),
            ], className="control-group"),

            # Clients selector
            html.Div([
                html.Label("Clients"),
                dcc.Dropdown(
                    id="parallel-selector",
                    options=[{"label": str(p), "value": p} for p in parallel_values],
                    value=parallel_values[0] if parallel_values else 1,
                    clearable=False,
                    style={"minWidth": "80px"},
                ),
            ], className="control-group"),

            # Metric radio buttons
            html.Div([
                html.Div([
                    html.Div("RPS", id="btn-rps", className="radio-item selected", n_clicks=0),
                    html.Div("Avg Latency", id="btn-mean_time", className="radio-item", n_clicks=0),
                    html.Div("p95 Latency", id="btn-p95_time", className="radio-item", n_clicks=0),
                    html.Div("Index Time", id="btn-upload_time", className="radio-item", n_clicks=0),
                ], className="radio-group"),
            ]),
        ], className="controls"),

        # Legend
        html.Div(id="legend-container", className="legend-hint"),

        # Chart
        html.Div([
            dcc.Graph(id="main-chart", config={"displayModeBar": False}),
        ], className="chart-container"),

        # Precision slider
        html.Div([
            dcc.Slider(
                id="precision-slider",
                min=0.5,
                max=1.0,
                step=0.01,
                value=0.9,
                marks={i/10: f"{i/10:.1f}" for i in range(5, 11)},
                tooltip={"placement": "bottom", "always_visible": True},
            ),
            html.Span("Drag to set minimum precision cutoff for the table below", className="slider-label"),
        ], className="precision-slider"),

        # Data table
        html.Div(id="data-table-container"),

        # Hidden store for selected metric
        dcc.Store(id="selected-metric", data="rps"),

    ], className="benchmark-wrapper"),
])


@callback(
    Output("selected-metric", "data"),
    Output("btn-rps", "className"),
    Output("btn-mean_time", "className"),
    Output("btn-p95_time", "className"),
    Output("btn-upload_time", "className"),
    Input("btn-rps", "n_clicks"),
    Input("btn-mean_time", "n_clicks"),
    Input("btn-p95_time", "n_clicks"),
    Input("btn-upload_time", "n_clicks"),
    State("selected-metric", "data"),
)
def update_metric_selection(n1, n2, n3, n4, current_metric):
    """Handle metric button clicks."""
    ctx = dash.callback_context
    if not ctx.triggered:
        return current_metric, "radio-item selected", "radio-item", "radio-item", "radio-item"

    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    metric_map = {
        "btn-rps": "rps",
        "btn-mean_time": "mean_time",
        "btn-p95_time": "p95_time",
        "btn-upload_time": "upload_time",
    }

    selected = metric_map.get(button_id, current_metric)
    classes = {k: "radio-item selected" if v == selected else "radio-item" for k, v in metric_map.items()}

    return selected, classes["btn-rps"], classes["btn-mean_time"], classes["btn-p95_time"], classes["btn-upload_time"]



@callback(
    Output("main-chart", "figure"),
    Output("legend-container", "children"),
    Input("dataset-selector", "value"),
    Input("parallel-selector", "value"),
    Input("selected-metric", "data"),
    Input("precision-slider", "value"),
)
def update_chart(dataset, parallel, metric, precision_cutoff):
    """Update the main chart based on selections."""
    if not dataset:
        return go.Figure(), []

    # Handle Index Time: join upload data with search data to get precision
    if metric == "upload_time":
        upload_df = df[(df["dataset"] == dataset) & (df["type"] == "upload")]
        search_df = df[(df["dataset"] == dataset) & (df["type"] == "search")]

        if upload_df.empty:
            return go.Figure().update_layout(title="No upload data available"), []

        # Get best precision per experiment from search results
        precision_by_exp = search_df.groupby("experiment")["mean_precisions"].max().to_dict()

        # Get fastest upload per experiment and add precision
        idx = upload_df.groupby("experiment")["total_time"].idxmin()
        upload_df = upload_df.loc[idx].copy()
        upload_df["mean_precisions"] = upload_df["experiment"].map(precision_by_exp)

        # Drop experiments without precision data
        upload_df = upload_df.dropna(subset=["mean_precisions"])

        if upload_df.empty:
            return go.Figure().update_layout(title="No matching upload/search data"), []

        fig = go.Figure()
        legend_items = []

        y_label = "Total Build Time (s)"

        # Aggregate by engine and apply Pareto filtering (lower time is better)
        for i, engine in enumerate(sorted(upload_df["engine"].unique())):
            engine_df = upload_df[upload_df["engine"] == engine].copy()

            # For build time, lower is better - use Pareto filtering
            engine_df = filter_pareto_points(engine_df, "total_time")
            engine_df = engine_df.sort_values("mean_precisions")

            color = get_engine_color(engine, i)

            fig.add_trace(go.Scatter(
                x=engine_df["mean_precisions"],
                y=engine_df["total_time"],
                mode="lines+markers",
                name=engine,
                line=dict(color=color, width=2.5, shape="spline"),
                marker=dict(size=8, color=color),
                hovertemplate=(
                    f"<b>{engine}</b><br>"
                    f"Precision: %{{x:.4f}}<br>"
                    f"{y_label}: %{{y:.1f}}s<br>"
                    f"Config: %{{customdata}}<extra></extra>"
                ),
                customdata=engine_df["experiment"],
            ))

            legend_items.append(html.Div([
                html.Span(className="legend-dot", style={"backgroundColor": color}),
                html.Span(engine),
            ], className="legend-item"))

        # Add precision cutoff line
        y_max = upload_df["total_time"].max() * 1.1
        fig.add_trace(go.Bar(
            name="Precision cutoff",
            x=[precision_cutoff],
            y=[y_max],
            marker_color="rgba(255, 0, 0, 0.2)",
            width=0.005,
            hoverinfo="skip",
        ))

        fig.update_layout(
            showlegend=False,
            plot_bgcolor="white",
            paper_bgcolor="white",
            barmode="overlay",
            xaxis=dict(title="Precision", gridcolor="#f1f3f8", range=[0.5, 1.0]),
            yaxis=dict(title=y_label, gridcolor="#f1f3f8", zeroline=False, rangemode="tozero"),
            margin=dict(l=60, r=20, t=20, b=40),
            hovermode="closest",
        )
        return fig, legend_items

    # Search metrics - line chart with precision on X-axis
    filtered_df = df[
        (df["dataset"] == dataset) &
        (df["type"] == "search") &
        (df["parallel"] == parallel)
    ]

    if filtered_df.empty:
        return go.Figure().update_layout(title="No search data available"), []

    fig = go.Figure()
    legend_items = []

    y_label = {
        "rps": "Queries Per Second",
        "mean_time": "Avg Latency (ms)",
        "p95_time": "p95 Latency (ms)",
    }.get(metric, metric)

    # =================================================================
    # Aggregate by ENGINE (not experiment) - combine all redis-* into "redis"
    # Apply Pareto filtering to ALL data points (like their JS implementation)
    # =================================================================
    for i, engine in enumerate(sorted(filtered_df["engine"].unique())):
        engine_df = filtered_df[filtered_df["engine"] == engine].copy()

        # Apply Pareto filtering directly to ALL points for this engine
        # This keeps the best trade-off points regardless of config
        engine_df = filter_pareto_points(engine_df, metric)
        engine_df = engine_df.sort_values("mean_precisions")

        # Get color based on engine name
        color = get_engine_color(engine, i)

        fig.add_trace(go.Scatter(
            x=engine_df["mean_precisions"],
            y=engine_df[metric],
            mode="lines+markers",
            name=engine,
            line=dict(color=color, width=2.5, shape="spline"),
            marker=dict(size=8, color=color),
            hovertemplate=(
                f"<b>{engine}</b><br>"
                f"Precision: %{{x:.4f}}<br>"
                f"{y_label}: %{{y:.2f}}<br>"
                f"ef/numCandidates: %{{customdata[0]}}<br>"
                f"Config: %{{customdata[1]}}<extra></extra>"
            ),
            customdata=list(zip(engine_df["search_ef"], engine_df["experiment"])),
        ))

        legend_items.append(html.Div([
            html.Span(className="legend-dot", style={"backgroundColor": color}),
            html.Span(engine),
        ], className="legend-item"))

    # Get y-axis max for the precision cutoff line
    y_max = filtered_df[metric].max() * 1.1

    # Add precision cutoff vertical line (like their red bar)
    fig.add_trace(go.Bar(
        name="Precision cutoff",
        x=[precision_cutoff],
        y=[y_max],
        marker_color="rgba(255, 0, 0, 0.2)",
        width=0.005,
        hoverinfo="skip",
    ))

    fig.update_layout(
        showlegend=False,
        plot_bgcolor="white",
        paper_bgcolor="white",
        barmode="overlay",
        xaxis=dict(
            title="Precision",
            gridcolor="#f1f3f8",
            range=[0.5, 1.0],
        ),
        yaxis=dict(
            title=y_label,
            gridcolor="#f1f3f8",
            zeroline=False,
            rangemode="tozero",
        ),
        margin=dict(l=60, r=20, t=20, b=40),
        hovermode="closest",
    )

    return fig, legend_items


@callback(
    Output("data-table-container", "children"),
    Input("dataset-selector", "value"),
    Input("parallel-selector", "value"),
    Input("precision-slider", "value"),
    Input("selected-metric", "data"),
)
def update_table(dataset, parallel, min_precision, metric):
    """Update the data table based on precision slider."""
    if not dataset or metric == "upload_time":
        return html.Div()

    filtered_df = df[
        (df["dataset"] == dataset) &
        (df["type"] == "search") &
        (df["parallel"] == parallel) &
        (df["mean_precisions"] >= min_precision)
    ]

    if filtered_df.empty:
        return html.Div("No data above precision threshold", style={"color": "#9ca3af", "textAlign": "center", "padding": "20px"})

    # Get best result per experiment (highest RPS)
    idx = filtered_df.groupby("experiment")["rps"].idxmax()
    best_df = filtered_df.loc[idx].sort_values("rps", ascending=False)

    rows = []
    for _, row in best_df.iterrows():
        rows.append(html.Tr([
            html.Td(row["experiment"]),
            html.Td(f"{row['mean_precisions']:.4f}"),
            html.Td(f"{row['rps']:.1f}"),
            html.Td(f"{row['mean_time']:.2f} ms"),
            html.Td(f"{row['p95_time']:.2f} ms"),
            html.Td(f"ef={int(row['search_ef'])}"),
        ]))

    return html.Table([
        html.Thead(html.Tr([
            html.Th("Experiment"),
            html.Th("Precision"),
            html.Th("RPS"),
            html.Th("Avg Latency"),
            html.Th("p95 Latency"),
            html.Th("Config"),
        ])),
        html.Tbody(rows),
    ], className="data-table")


if __name__ == "__main__":
    app.run(debug=True, port=8055)

