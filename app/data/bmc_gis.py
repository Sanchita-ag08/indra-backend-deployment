"""
BMC/MCGM public GIS connector.

Official MCGM ArcGIS REST service.

Core infrastructure layers:
- Storm Water Manholes: 6
- Storm Water Drains: 7
- Wards: 238
- Mumbai Contour: 217

Flooding spots / flow sensors are treated as optional.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

import httpx


BASE = (
    "https://prsrvgisapp.mcgm.gov.in/server/rest/services/mcgm/"
    "MCGMGIS_Departments_Master_All_Layers/MapServer"
)

LAYERS = {
    "storm_water_manholes": 6,
    "storm_water_drains": 7,
    "wards": 238,
    "flooding_spots": 344,
    "flow_level_sensors": 345,
    "vulnerable_settlements": 346,
    "mumbai_contour": 217,
}


async def query_layer(
    layer: str,
    where: str = "1=1",
    out_fields: str = "*",
    out_sr: int = 4326,
    result_record_count: int = 100,
    result_offset: int = 0,                     # NEW
    geometry: Optional[str] = None,
    geometry_type: Optional[str] = None,
    in_sr: Optional[int] = None,
    max_allowable_offset: Optional[float] = None,  # NEW (drainage_graph.py isko pass karta hai)
) -> dict[str, Any]:

    if layer not in LAYERS:
        raise ValueError(f"Unknown BMC layer: {layer}")

    url = f"{BASE}/{LAYERS[layer]}/query"

    params: dict[str, str] = {
        "where": where,
        "outFields": out_fields,
        "f": "geojson",
        "outSR": str(out_sr),
        "returnGeometry": "true",
        "resultRecordCount": str(result_record_count),
        "resultOffset": str(result_offset),          # NEW
    }

    if max_allowable_offset is not None:              # NEW
        params["maxAllowableOffset"] = str(max_allowable_offset)

    if geometry:
        params["geometry"] = geometry
    if geometry_type:
        params["geometryType"] = geometry_type
    if in_sr:
        params["inSR"] = str(in_sr)

    last_error: Exception | None = None
    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(url, params=params)

            if response.status_code == 404:
                raise RuntimeError(
                    f"BMC GIS layer unavailable: {layer} (layer id {LAYERS[layer]})"
                )

            response.raise_for_status()
            payload = response.json()

            if "error" in payload:
                raise RuntimeError(payload["error"])

            return payload

        except httpx.TimeoutException as exc:
            last_error = RuntimeError(
                f"BMC GIS request timed out for {layer} (layer id {LAYERS[layer]})"
            )
        except (httpx.HTTPStatusError, httpx.TransportError) as exc:
            last_error = exc

        if attempt < 2:
            await asyncio.sleep(0.6 * (attempt + 1))  # 0.6s, then 1.2s

    raise last_error

# -------------------------------------------------------------------
# Core drainage layers
# -------------------------------------------------------------------

async def get_storm_water_drains() -> dict[str, Any]:
    return await query_layer(
        "storm_water_drains",
        result_record_count=100,
    )


async def get_storm_water_manholes() -> dict[str, Any]:
    return await query_layer(
        "storm_water_manholes",
        result_record_count=100,
    )


# -------------------------------------------------------------------
# Supporting layers
# -------------------------------------------------------------------

async def get_wards() -> dict[str, Any]:
    return await query_layer(
        "wards",
        result_record_count=100,
    )


async def get_flooding_spots() -> dict[str, Any]:
    return await query_layer(
        "flooding_spots",
        result_record_count=100,
    )


async def get_flow_level_sensors() -> dict[str, Any]:
    return await query_layer(
        "flow_level_sensors",
        result_record_count=100,
    )


async def get_vulnerable_settlements() -> dict[str, Any]:
    return await query_layer(
        "vulnerable_settlements",
        result_record_count=100,
    )


async def get_mumbai_contour() -> dict[str, Any]:
    return await query_layer(
        "mumbai_contour",
        result_record_count=100,
    )


# -------------------------------------------------------------------
# Source metadata
# -------------------------------------------------------------------

def source_metadata() -> dict[str, Any]:
    return {
        "provider": "MCGM / BMC",
        "service": BASE,
        "layers": LAYERS,
        "status": "official_bmc_arcgis_rest",
    }