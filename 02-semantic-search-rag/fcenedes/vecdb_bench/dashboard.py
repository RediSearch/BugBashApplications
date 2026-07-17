#!/usr/bin/env python3
"""
Vector DB Benchmark Dashboard

A Dash-based dashboard for visualizing benchmark results.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any

import dash
from dash import dcc, html, callback, Input, Output
import plotly.express as px
import pandas as pd


def load_results(results_dir: str = "results/") -> pd.DataFrame:
    """Load all benchmark results from JSON files into a DataFrame."""
    records = []
    
    for filepath in Path(results_dir).glob("*.json"):
        filename = filepath.name
        
        # Skip summary files for now - we'll use individual results
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
        
        # Determine if this is a search or upload result
        if "search" in filename:
            record["type"] = "search"
            record["parallel"] = params.get("parallel", 1)
            # =================================================================
            # FIX: Handle different search params for different engines
            # Redis uses: search_params.ef
            # MongoDB uses: search_params.numCandidates
            # =================================================================
            search_params = params.get("search_params", {})
            # Try Redis ef first, then MongoDB numCandidates
            ef_runtime = search_params.get("ef", 0)
            if ef_runtime == 0:
                ef_runtime = search_params.get("numCandidates", 0)
            record["search_ef"] = ef_runtime
            record["rps"] = results.get("rps", 0)
            record["mean_time"] = results.get("mean_time", 0) * 1000  # Convert to ms
            record["p95_time"] = results.get("p95_time", 0) * 1000  # Convert to ms
            record["p99_time"] = results.get("p99_time", 0) * 1000  # Convert to ms
            record["mean_precisions"] = results.get("mean_precisions", 0)
        elif "upload" in filename:
            record["type"] = "upload"
            record["parallel"] = params.get("parallel", 1)
            record["upload_time"] = results.get("upload_time", 0)
            record["total_time"] = results.get("total_time", 0)
            memory_usage = results.get("memory_usage", {})

            # =================================================================
            # Memory breakdown for Redis:
            # - used_memory: Total Redis memory (bytes)
            # - index_info.total_index_memory_sz_mb: Index memory (MB)
            # - Data memory = used_memory - index_memory
            # MongoDB doesn't report memory, so all values will be 0
            # =================================================================
            used_memory_bytes = memory_usage.get("used_memory", [0])
            used_memory_bytes = used_memory_bytes[0] if used_memory_bytes else 0

            index_info = memory_usage.get("index_info", {})
            index_memory_mb = float(index_info.get("total_index_memory_sz_mb", 0) or 0)
            index_memory_bytes = index_memory_mb * 1024 * 1024

            # Calculate data memory (total - index)
            data_memory_bytes = max(0, used_memory_bytes - index_memory_bytes)

            record["total_memory_gb"] = used_memory_bytes / (1024 ** 3)
            record["index_memory_gb"] = index_memory_bytes / (1024 ** 3)
            record["data_memory_gb"] = data_memory_bytes / (1024 ** 3)
            # Keep memory_gb for backward compatibility
            record["memory_gb"] = record["total_memory_gb"]
        else:
            continue
            
        records.append(record)
    
    return pd.DataFrame(records)


def get_unique_values(df: pd.DataFrame, column: str) -> List[str]:
    """Get unique non-empty values from a column."""
    return sorted([v for v in df[column].unique() if v])


# Load data
df = load_results()

# Get unique values for dropdowns
datasets = get_unique_values(df, "dataset")
experiments = get_unique_values(df, "experiment")
parallel_values = sorted(df[df["type"] == "search"]["parallel"].unique())

# Metric options
METRIC_OPTIONS = [
    {"label": "Queries Per Second (RPS)", "value": "rps"},
    {"label": "Average Latency (ms)", "value": "mean_time"},
    {"label": "P95 Latency (ms)", "value": "p95_time"},
    {"label": "Index Build Time (s)", "value": "build_time"},  # Stacked bar: upload + indexing
    {"label": "Total Memory Usage (GB)", "value": "memory_gb"},
]

# Create a lookup dict for metric labels
METRIC_LABELS = {opt["value"]: opt["label"] for opt in METRIC_OPTIONS}

# Create Dash app
app = dash.Dash(__name__, title="Vector DB Benchmark Dashboard")

app.layout = html.Div([
    html.H1("Vector DB Benchmark Dashboard", style={"textAlign": "center"}),
    
    html.Div([
        html.Div([
            html.Label("Dataset:"),
            dcc.Dropdown(
                id="dataset-dropdown",
                options=[{"label": d, "value": d} for d in datasets],
                value=datasets[0] if datasets else None,
                clearable=False,
            ),
        ], style={"width": "30%", "display": "inline-block", "padding": "10px"}),

        html.Div([
            html.Label("Number of Clients (Parallel):"),
            dcc.Dropdown(
                id="parallel-dropdown",
                options=[{"label": str(p), "value": p} for p in parallel_values],
                value=parallel_values[0] if parallel_values else None,
                clearable=False,
            ),
        ], style={"width": "30%", "display": "inline-block", "padding": "10px"}),

        html.Div([
            html.Label("Metric:"),
            dcc.Dropdown(
                id="metric-dropdown",
                options=METRIC_OPTIONS,
                value="rps",
                clearable=False,
            ),
        ], style={"width": "30%", "display": "inline-block", "padding": "10px"}),
    ], style={"display": "flex", "justifyContent": "center"}),
    
    html.Div([
        dcc.Graph(id="benchmark-graph", style={"height": "70vh"}),
    ]),
    
    html.Div([
        html.P("Each point represents a configuration (build params + ef_search). Lines connect points from the same build config. Hover for details.",
               style={"textAlign": "center", "color": "gray"}),
    ]),
], style={"padding": "20px"})


@callback(
    Output("benchmark-graph", "figure"),
    [Input("dataset-dropdown", "value"),
     Input("parallel-dropdown", "value"),
     Input("metric-dropdown", "value")]
)
def update_graph(dataset: str, parallel: int, metric: str):
    """Update the graph based on selected filters."""
    if not dataset:
        return px.line(title="No data available")

    # Handle upload metrics separately (no precision axis)
    if metric in ["build_time", "memory_gb"]:
        filtered_df = df[(df["dataset"] == dataset) & (df["type"] == "upload")]
        if filtered_df.empty:
            return px.bar(title=f"No upload data available for {dataset}")

        # =================================================================
        # Build Time: Stacked bar chart showing upload_time + indexing_time
        # Memory: Simple bar chart with average
        # =================================================================
        if metric == "build_time":
            # Get fastest run per experiment (by total_time)
            idx = filtered_df.groupby("experiment")["total_time"].idxmin()
            agg_df = filtered_df.loc[idx].copy()

            # Calculate indexing time (total - upload)
            agg_df["indexing_time"] = agg_df["total_time"] - agg_df["upload_time"]

            # Reshape for stacked bar chart
            stacked_df = pd.melt(
                agg_df,
                id_vars=["experiment"],
                value_vars=["upload_time", "indexing_time"],
                var_name="phase",
                value_name="time_seconds"
            )

            # Rename phases for display
            stacked_df["phase"] = stacked_df["phase"].map({
                "upload_time": "Upload (data insertion)",
                "indexing_time": "Indexing (index build)"
            })

            fig = px.bar(
                stacked_df,
                x="experiment",
                y="time_seconds",
                color="phase",
                title=f"Index Build Time by Build Config (Fastest Run)",
                labels={"experiment": "Build Config", "time_seconds": "Time (seconds)", "phase": "Phase"},
                barmode="stack",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=True, legend_title="Phase")
        else:
            # =================================================================
            # Memory: Stacked bar chart showing data + index memory
            # For Redis: data_memory_gb + index_memory_gb = total_memory_gb
            # For MongoDB: No memory data available (all zeros)
            # =================================================================
            # Get average per experiment
            agg_df = filtered_df.groupby("experiment").agg({
                "data_memory_gb": "mean",
                "index_memory_gb": "mean",
                "total_memory_gb": "mean",
            }).reset_index()

            # Check if we have any memory data
            if agg_df["total_memory_gb"].sum() > 0:
                # Reshape for stacked bar chart
                stacked_df = pd.melt(
                    agg_df,
                    id_vars=["experiment"],
                    value_vars=["data_memory_gb", "index_memory_gb"],
                    var_name="memory_type",
                    value_name="memory_gb"
                )

                # Rename for display
                stacked_df["memory_type"] = stacked_df["memory_type"].map({
                    "data_memory_gb": "Data (vectors + metadata)",
                    "index_memory_gb": "Index (HNSW graph)"
                })

                fig = px.bar(
                    stacked_df,
                    x="experiment",
                    y="memory_gb",
                    color="memory_type",
                    title=f"Memory Usage by Build Config (Average)",
                    labels={"experiment": "Build Config", "memory_gb": "Memory (GB)", "memory_type": "Type"},
                    barmode="stack",
                )
                fig.update_layout(xaxis_tickangle=-45, showlegend=True, legend_title="Memory Type")
            else:
                # No memory data - show empty chart with message
                fig = px.bar(
                    agg_df,
                    x="experiment",
                    y="total_memory_gb",
                    color="experiment",
                    title=f"Memory Usage by Build Config (No data available)",
                    labels={"experiment": "Build Config", "total_memory_gb": "Memory (GB)"},
                )
                fig.update_layout(xaxis_tickangle=-45, showlegend=True, legend_title="Build Config")

        return fig

    # Search metrics - line chart with precision on X-axis
    filtered_df = df[
        (df["dataset"] == dataset) &
        (df["parallel"] == parallel) &
        (df["type"] == "search")
    ]
    if filtered_df.empty:
        return px.line(title=f"No search data for {dataset} with {parallel} clients")

    # Aggregate by experiment and search_ef (take mean of multiple runs)
    agg_df = filtered_df.groupby(["experiment", "search_ef"]).agg({
        "rps": "mean",
        "mean_precisions": "mean",
        "mean_time": "mean",
        "p95_time": "mean",
    }).reset_index()

    # Sort by precision for proper line drawing
    agg_df = agg_df.sort_values(["experiment", "mean_precisions"])

    fig = px.line(
        agg_df,
        x="mean_precisions",
        y=metric,
        color="experiment",
        markers=True,
        title=f"Precision vs {METRIC_LABELS[metric]} ({parallel} clients)",
        labels={
            "mean_precisions": "Precision",
            metric: METRIC_LABELS[metric],
            "experiment": "Build Config",
        },
        hover_data=["search_ef", "rps", "mean_time", "p95_time"],
    )

    fig.update_traces(marker=dict(size=10))

    fig.update_layout(
        showlegend=True,
        legend_title="Build Config",
        xaxis=dict(tickformat=".0%"),
    )

    return fig


if __name__ == "__main__":
    print("Starting Vector DB Benchmark Dashboard...")
    print(f"Loaded {len(df)} result records")
    print(f"Datasets: {datasets}")
    print(f"Experiments: {experiments}")
    print(f"Parallel values: {parallel_values}")
    app.run(debug=True, host="0.0.0.0", port=8050)

