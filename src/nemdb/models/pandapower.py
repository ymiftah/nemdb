import geopandas as gpd
import pandas as pd
import shapely as shp
from sklearn.cluster import DBSCAN

from nemdb.geodata.geodata import (
    read_major_powerstations,
    read_substations,
    read_transmission_lines,
)

METRIC_CRS = "EPSG:7856"
GEO_CRS = "EPSG:4326"


def get_pandapower_model():
    lines, buses, mapping = _get_buses_and_lines()

    pf_lines = _get_lines_pp(lines, mapping)
    pf_buses = _get_bus_pp(pf_lines, buses)
    pf_trafos = _get_trafos_pp(pf_buses)
    pf_gens = _get_gens_pp(pf_buses)
    pf_loads = _get_loads_pp(pf_buses)
    return {
        "buses": pf_buses,
        "lines": pf_lines,
        "trafos": pf_trafos,
        "gens": pf_gens,
        "loads": pf_loads,
    }


def _get_buses_and_lines():
    # Extract line segments
    lines = (
        read_transmission_lines(clean=True)
        .reset_index(drop=True)
        .rename_axis("line_id")
        .reset_index("line_id")
        .explode()
    )
    lines = lines.to_crs(METRIC_CRS)
    lines["length_km"] = shp.length(lines.geometry) / 1000

    # Extract buses from extremeties
    lines["start_point"] = lines.geometry.map(
        lambda linestringgeometry: shp.get_point(linestringgeometry, 0)
    )
    lines["end_point"] = lines.geometry.map(
        lambda linestringgeometry: shp.get_point(linestringgeometry, -1)
    )

    extremeties: gpd.GeoDataFrame = (
        gpd.GeoDataFrame(
            geometry=lines["start_point"].drop_duplicates().to_list()
            + lines["end_point"].drop_duplicates().to_list(),
            crs=lines.crs,
        )
        .drop_duplicates()
        .reset_index(names=["extremeties_id"])
        .assign(x=lambda x: x.geometry.x, y=lambda x: x.geometry.y)
    )

    # join extremities together into buses using a DBSCAN algorithm
    fitted_ = DBSCAN(eps=500, min_samples=1).fit(extremeties[["x", "y"]])
    extremeties["bus_id"] = "bus_" + fitted_.labels_.astype(str)
    buses = (
        extremeties.groupby("bus_id", as_index=False)
        .agg({"geometry": lambda s: shp.centroid(shp.MultiPoint(s))})
        .set_geometry("geometry", crs=METRIC_CRS)
    )

    mapping = extremeties.to_crs(GEO_CRS).set_index("geometry")["bus_id"]
    return lines, buses, mapping


def _get_lines_pp(lines, mapping):
    rows = []
    for _, row in lines.to_crs(GEO_CRS).iterrows():
        start_point = shp.get_point(row.geometry, 0)
        end_point = shp.get_point(row.geometry, -1)
        from_bus = mapping[start_point]
        to_bus = mapping[end_point]
        rows.append(
            {
                "name": row["name"],
                "from_bus": from_bus,
                "to_bus": to_bus,
                "length_km": row["length_km"],
                "in_service": row["operationalstatus"] == "Operational",
                "class": row["class"],
                "geodata": row["geometry"].coords,
                "voltagekv": row["capacitykv"],
            }
        )
    df = pd.DataFrame(rows)
    df = df[df["to_bus"] != df["from_bus"]]  # Remove lines going to the same bus
    df["from_bus"] = df["from_bus"] + "_" + df["voltagekv"].astype(str) + "kv"
    df["to_bus"] = df["to_bus"] + "_" + df["voltagekv"].astype(str) + "kv"

    return df


def _get_bus_pp(pf_lines, buses):
    pf_buses = (
        pd.concat(
            (
                pf_lines[["from_bus", "voltagekv"]].rename(columns={"from_bus": "bus"}),
                pf_lines[["to_bus", "voltagekv"]].rename(columns={"to_bus": "bus"}),
            ),
            axis=0,
        )
        .drop_duplicates()
        .sort_values("bus")
        .reset_index(drop=True)
        .assign(in_service=True)
        .rename(columns={"voltagekv": "vn_kv", "bus": "bus_id"})
    )
    pf_buses["geodata"] = (
        pf_buses["bus_id"]
        .str.rsplit("_", n=1, expand=True)[0]
        .map(buses.to_crs(GEO_CRS).set_index("bus_id")["geometry"].to_dict())
    )
    return pf_buses


def _get_trafos_pp(pf_buses):
    gdf = (
        pf_buses["bus_id"]
        .str.rsplit("_", n=1, expand=True)
        .set_axis(["bus_id", "vn_kv"], axis=1)
        .groupby("bus_id")
        .agg(lambda s: sorted(s.str.replace("kv", "").astype(int)))
    )
    gdf = gdf[gdf["vn_kv"].map(len) > 1].reset_index()

    trafos = []
    for _, row in gdf.iterrows():
        bus = row["bus_id"]
        for lv, hv in zip(row["vn_kv"][:-1], row["vn_kv"][1:], strict=False):
            trafos.append(
                {
                    "name": "trafo" + f"_{bus}_{lv}kv_to_{bus}_{hv}kv",
                    "lv_bus": bus + f"_{lv}kv",
                    "hv_bus": bus + f"_{hv}kv",
                    "vn_lv_kv": lv,
                    "vn_hv_kv": hv,
                    # ARRBITRARY VALUES TODO find acceptable values
                    "sn_mva": 1_000,
                    "vk_percent": 12.2,
                    "vkr_percent": 0.25,
                    "pfe_kw": 60.0,
                    "i0_percent": 0.06,
                    "in_service": True,
                }
            )
    pf_trafos = pd.DataFrame(trafos)
    return pf_trafos


def _get_gens_pp(pf_buses: pd.DataFrame):
    gens = (
        read_major_powerstations()
        .reset_index(drop=True)
        .to_crs(METRIC_CRS)[
            [
                "featuretype",
                "class",
                "name",
                "operationalstatus",
                "owner",
                "generationtype",
                "primaryfueltype",
                "generationmw",
                "ga_guid",
                "state",
                "x_coordinate",
                "y_coordinate",
                "geometry",
            ]
        ]
    )
    pf_gens = (
        gens.sjoin_nearest(
            gpd.GeoDataFrame(
                pf_buses[["bus_id", "vn_kv", "geodata"]],
                geometry="geodata",
                crs=GEO_CRS,
            ).to_crs(METRIC_CRS)
        )
        .assign(
            name=lambda s: s["name"],
            p_mw=lambda s: s["generationmw"] / 2,
            max_p_mw=lambda s: s["generationmw"],
            type=lambda s: s["generationtype"],
            fueltype=lambda s: s["primaryfueltype"],
            in_service=lambda s: s["operationalstatus"] == "Operational",
        )[
            [
                "bus_id",
                "name",
                "p_mw",
                "max_p_mw",
                "type",
                "in_service",
                "owner",
                "fueltype",
                "vn_kv",
                "geometry",
            ]
        ]
        .sort_values(["name", "vn_kv"])
        .drop_duplicates(["name", "max_p_mw"], keep="last")
        .dropna(subset=["max_p_mw"])
        .to_crs(GEO_CRS)
        .rename(columns={"geometry": "geodata"})
    )
    return pf_gens


def _get_loads_pp(pf_buses: pd.DataFrame):
    subs = read_substations().reset_index(drop=True).to_crs(METRIC_CRS)
    subs["name"] = subs["name"].fillna("NA_ZSS_" + subs["locality"])
    subs = subs[
        [
            "class",
            "name",
            "operationalstatus",
            "state",
            "locality",
            "voltagekv",
            "geometry",
            "ga_guid",
        ]
    ]
    pf_subs = (
        subs.sjoin_nearest(
            gpd.GeoDataFrame(
                pf_buses[["bus_id", "vn_kv", "geodata"]],
                geometry="geodata",
                crs=GEO_CRS,
            ).to_crs(METRIC_CRS),
        )
        .query("voltagekv == vn_kv")
        .drop_duplicates(subset=["name", "class", "ga_guid"], keep="first")
        .dropna(subset="bus_id")
        .assign(
            name=lambda s: s["name"],
            type=lambda s: s["class"],
            in_service=lambda s: s["operationalstatus"] == "Operational",
        )[
            [
                "bus_id",
                "name",
                "type",
                "vn_kv",
                "in_service",
                "locality",
                "state",
                "geometry",
            ]
        ]
        .to_crs(GEO_CRS)
        .rename(columns={"geometry": "geodata"})
    )
    return pf_subs
