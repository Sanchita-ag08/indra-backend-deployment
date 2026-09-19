"""
Builds a real drainage-network graph (nodes = BMC storm-water manholes,
edges = BMC storm-water drains) and simulates per-edge/per-node capacity
utilization driven by current rainfall intensity.

IMPORTANT: Node and edge POSITIONS/CONNECTIVITY are real BMC GIS data.
Capacity (litres/sec) is NOT measured — real per-pipe capacity sensors
are not yet deployed — so it is generated with a deterministic seeded
random value per edge (stable across refreshes, never silently changes
on its own). This is clearly flagged in the response as
"capacity_source": "seeded_simulation" so it is never confused with an
observed value.

PERFORMANCE NOTE: fetching + spatially matching the entire BMC layer is
expensive (many paginated HTTP calls + O(nodes x edges) nearest-neighbor
matching). Positions/connectivity/seeded capacity almost never change, so
that "structural" part is built once and cached in memory
(STRUCTURE_CACHE_TTL_SECONDS). Only the cheap rainfall-driven flow/status
recompute happens on every call.
"""
from __future__ import annotations

import asyncio
import hashlib
import random
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.data import bmc_gis
from app.db.models import RainfallObservation
from app.intelligence.drainage import utilization

# Endpoints within this distance (degrees; ~65m at Mumbai's latitude)
# are treated as "the same manhole" when linking a drain line to nodes.
MATCH_TOLERANCE_DEG = 0.0006

# Plausible drain capacity range in litres/second (seeded-random, not measured)
CAPACITY_MIN_LPS = 400.0
CAPACITY_MAX_LPS = 2200.0

# Baseline (dry-weather) flow range, litres/second
BASE_FLOW_MIN_LPS = 5.0
BASE_FLOW_MAX_LPS = 40.0

# How strongly rainfall drives simulated flow — lps added per mm/hr,
# scaled per-edge by a seeded catchment-size multiplier
RAIN_RESPONSE_MIN = 3.0
RAIN_RESPONSE_MAX = 22.0

# Page size used when paginating BMC ArcGIS layers.
GIS_PAGE_SIZE = 100

# How long the fetched-from-BMC structural graph (positions, connectivity,
# seeded capacity/base-flow/rain-response) stays cached before being
# re-pulled. Real infrastructure positions don't change minute to minute,
# so this can safely be long — only rainfall-driven flow recomputes live.
STRUCTURE_CACHE_TTL_SECONDS = 3600  # 1 hour

_structure_cache: dict[str, Any] = {"built_at": 0.0, "nodes": None, "edges_static": None}
_structure_lock = asyncio.Lock()


def _seeded_rng(key: str) -> random.Random:
    """Deterministic RNG per key, so values stay stable across refreshes."""
    h = hashlib.sha256(key.encode()).hexdigest()
    return random.Random(int(h[:16], 16))


def _feature_id(feature: dict, prefix: str, index: int) -> str:
    props = feature.get("properties") or {}
    raw = (
        props.get("OBJECTID")
        or props.get("objectid")
        or props.get("Id")
        or props.get("FID")
    )
    return f"{prefix}_{raw}" if raw is not None else f"{prefix}_{index}"


def _point_coords(feature: dict) -> tuple[float, float] | None:
    geom = feature.get("geometry") or {}
    if geom.get("type") != "Point":
        return None
    coords = geom.get("coordinates")
    if not coords or len(coords) < 2:
        return None
    return float(coords[0]), float(coords[1])  # (lon, lat)


def _line_coords(feature: dict) -> list[list[float]] | None:
    geom = feature.get("geometry") or {}
    gtype = geom.get("type")
    if gtype == "LineString":
        return geom.get("coordinates")
    if gtype == "MultiLineString":
        parts = geom.get("coordinates") or []
        flat: list[list[float]] = []
        for part in parts:
            flat.extend(part)
        return flat or None
    return None


# ---------------------------------------------------------------------
# Fast nearest-node lookup: instead of scanning every node for every
# drain endpoint (O(nodes x edges)), bucket nodes into a coarse grid
# keyed by rounded lon/lat, then only check the node's own cell + the
# 8 neighboring cells. This turns matching into ~O(edges) on average.
# ---------------------------------------------------------------------

def _grid_key(lon: float, lat: float) -> tuple[int, int]:
    return (round(lon / MATCH_TOLERANCE_DEG), round(lat / MATCH_TOLERANCE_DEG))


def _build_node_grid(nodes: list[dict]) -> dict[tuple[int, int], list[dict]]:
    grid: dict[tuple[int, int], list[dict]] = {}
    for n in nodes:
        key = _grid_key(n["longitude"], n["latitude"])
        grid.setdefault(key, []).append(n)
    return grid


def _nearest_node_indexed(lon: float, lat: float, grid: dict[tuple[int, int], list[dict]]) -> dict | None:
    gx, gy = _grid_key(lon, lat)
    best, best_d = None, MATCH_TOLERANCE_DEG
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            for n in grid.get((gx + dx, gy + dy), ()):
                d = ((n["longitude"] - lon) ** 2 + (n["latitude"] - lat) ** 2) ** 0.5
                if d <= best_d:
                    best, best_d = n, d
    return best


async def _fetch_all_features(layer_name: str, page_size: int = GIS_PAGE_SIZE, **extra: Any) -> list[dict]:
    """
    Fetches an ENTIRE BMC ArcGIS layer by paging through resultOffset,
    instead of relying on a single request (which silently truncates to
    whatever record count/limit the server enforces). Stops as soon as a
    page comes back shorter than page_size, which is the standard ArcGIS
    "no more records" signal.
    """
    features: list[dict] = []
    offset = 0
    while True:
        fc = await bmc_gis.query_layer(
            layer_name,
            result_record_count=page_size,
            result_offset=offset,
            **extra,
        )
        page = fc.get("features") or []
        features.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return features


def _current_rainfall_mm_hr(db: Session) -> float:
    """
    Uses the same seeded RainfallObservation data that already powers
    the rest of INDRA's risk map, so this graph's simulated flow stays
    consistent with what the dashboard/map already show.
    """
    latest = db.query(RainfallObservation).order_by(desc(RainfallObservation.timestamp)).first()
    if not latest:
        return 0.0
    same_batch = db.query(RainfallObservation).filter(
        RainfallObservation.timestamp == latest.timestamp
    ).all()
    values = [r.rain_60m_mm for r in same_batch if r.rain_60m_mm is not None]
    return round(sum(values) / len(values), 1) if values else 0.0


async def _build_structural_graph() -> tuple[list[dict], list[dict]]:
    """
    The expensive, rarely-changing part: fetch BMC manholes + drains
    (concurrently) and spatially link them. Returns (nodes, edges_static)
    where edges_static carries id/from_node/to_node/coordinates/capacity/
    base_flow/rain_response — everything EXCEPT the rainfall-dependent
    simulated_flow/utilization/status, which build_drainage_graph()
    computes cheaply on top of this on every call.
    """
    
    manhole_features, drain_features = await asyncio.gather(
    _fetch_all_features(
        "storm_water_manholes",
        page_size=100,
        out_fields="OBJECTID,NODE_ID",
    ),
    _fetch_all_features(
        "storm_water_drains",
        page_size=100,
        out_fields="OBJECTID,US_NODE_ID,DS_NODE_ID",
        max_allowable_offset=0.00005,
    ),
    return_exceptions=True,
)
    # if isinstance(manhole_features, BaseException):
    #     manhole_features = []
    # if isinstance(drain_features, BaseException):
    #     drain_features = []
    if isinstance(manhole_features, BaseException):
        print(f"[DRAINAGE GRAPH] Manhole query failed: {manhole_features}")
        manhole_features = []

    if isinstance(drain_features, BaseException):
        print(f"[DRAINAGE GRAPH] Drain query failed: {drain_features}")
        drain_features = []
    nodes: list[dict[str, Any]] = []
    for i, feature in enumerate(manhole_features):
        pt = _point_coords(feature)
        if not pt:
            continue
        lon, lat = pt
        nodes.append({
            "id": _feature_id(feature, "MH", i),
            "longitude": lon,
            "latitude": lat,
        })

    grid = _build_node_grid(nodes)

    edges_static: list[dict[str, Any]] = []
    for i, feature in enumerate(drain_features):
        coords = _line_coords(feature)
        if not coords or len(coords) < 2:
            continue

        edge_id = _feature_id(feature, "DRAIN", i)
        start_lon, start_lat = coords[0][0], coords[0][1]
        end_lon, end_lat = coords[-1][0], coords[-1][1]

        from_node = _nearest_node_indexed(start_lon, start_lat, grid)
        to_node = _nearest_node_indexed(end_lon, end_lat, grid)

        rng = _seeded_rng(edge_id)
        edges_static.append({
            "id": edge_id,
            "from_node": from_node["id"] if from_node else None,
            "to_node": to_node["id"] if to_node else None,
            "coordinates": coords,
            "capacity_lps": rng.uniform(CAPACITY_MIN_LPS, CAPACITY_MAX_LPS),
            "base_flow_lps": rng.uniform(BASE_FLOW_MIN_LPS, BASE_FLOW_MAX_LPS),
            "rain_response_lps": rng.uniform(RAIN_RESPONSE_MIN, RAIN_RESPONSE_MAX),
        })

    return nodes, edges_static


async def _get_structural_graph(force_refresh: bool = False) -> tuple[list[dict], list[dict]]:
    now = time.monotonic()
    fresh = (
        not force_refresh
        and _structure_cache["nodes"] is not None
        and (now - _structure_cache["built_at"]) < STRUCTURE_CACHE_TTL_SECONDS
    )
    if fresh:
        return _structure_cache["nodes"], _structure_cache["edges_static"]

    async with _structure_lock:
        # Re-check after acquiring the lock — another request may have
        # already rebuilt it while we were waiting.
        now = time.monotonic()
        fresh = (
            not force_refresh
            and _structure_cache["nodes"] is not None
            and (now - _structure_cache["built_at"]) < STRUCTURE_CACHE_TTL_SECONDS
        )
        if fresh:
            return _structure_cache["nodes"], _structure_cache["edges_static"]

        nodes, edges_static = await _build_structural_graph()
        _structure_cache["nodes"] = nodes
        _structure_cache["edges_static"] = edges_static
        _structure_cache["built_at"] = time.monotonic()
        return nodes, edges_static


async def build_drainage_graph(db: Session, force_refresh: bool = False) -> dict[str, Any]:
    nodes_static, edges_static = await _get_structural_graph(force_refresh=force_refresh)
    rainfall_mm_hr = _current_rainfall_mm_hr(db)

    nodes: list[dict[str, Any]] = [
        {**n, "utilization_pct": 0.0, "status": "normal"} for n in nodes_static
    ]
    node_max_ratio: dict[str, float] = {}

    edges: list[dict[str, Any]] = []
    for e in edges_static:
        simulated_flow = e["base_flow_lps"] + e["rain_response_lps"] * rainfall_mm_hr
        ratio = utilization(simulated_flow, e["capacity_lps"])
        pct = round(ratio * 100, 1)
        status = (
            "critical" if ratio >= 1 else
            "high" if ratio >= .85 else
            "loaded" if ratio >= .65 else
            "normal"
        )

        edges.append({
            "id": e["id"],
            "from_node": e["from_node"],
            "to_node": e["to_node"],
            "coordinates": e["coordinates"],
            "capacity_lps": round(e["capacity_lps"], 1),
            "simulated_flow_lps": round(simulated_flow, 1),
            "utilization_pct": pct,
            "status": status,
        })

        for node_id in (e["from_node"], e["to_node"]):
            if node_id:
                node_max_ratio[node_id] = max(node_max_ratio.get(node_id, 0.0), ratio)

    for n in nodes:
        ratio = node_max_ratio.get(n["id"], 0.0)
        n["utilization_pct"] = round(ratio * 100, 1)
        n["status"] = (
            "critical" if ratio >= 1 else
            "high" if ratio >= .85 else
            "loaded" if ratio >= .65 else
            "normal"
        )

    summary = {
        "normal": sum(1 for e in edges if e["status"] == "normal"),
        "loaded": sum(1 for e in edges if e["status"] == "loaded"),
        "high": sum(1 for e in edges if e["status"] == "high"),
        "critical": sum(1 for e in edges if e["status"] == "critical"),
        "total_edges": len(edges),
        "total_nodes": len(nodes),
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rainfall_mm_hr_used": rainfall_mm_hr,
        "capacity_source": "seeded_simulation",
        "nodes": nodes,
        "edges": edges,
        "summary": summary,
    }