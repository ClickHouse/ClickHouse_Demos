#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import struct
from dataclasses import dataclass
from pathlib import Path


# Inverse projection for:
# PROJCS["NAD_1983_StatePlane_New_York_Long_Island_FIPS_3104_Feet", ... PROJECTION["Lambert_Conformal_Conic"], ...]
#
# Source: standard Lambert Conformal Conic (2SP) inverse formulas (ellipsoidal)
# using GRS80 (NAD83) parameters from the .prj.
#
# NOTE: This is "good enough" for demo visualization (MapLibre). It should
# match typical GIS reprojection closely for these datasets.


@dataclass(frozen=True)
class Lcc2sp:
    a: float  # semi-major axis (meters)
    f_inv: float  # inverse flattening
    phi1: float  # standard parallel 1 (radians)
    phi2: float  # standard parallel 2 (radians)
    phi0: float  # latitude of origin (radians)
    lam0: float  # central meridian (radians)
    fe: float  # false easting (meters)
    fn: float  # false northing (meters)


def _deg(d: float) -> float:
    return d * math.pi / 180.0


def _rad(r: float) -> float:
    return r * 180.0 / math.pi


def _eccentricity(f_inv: float) -> float:
    f = 1.0 / f_inv
    return math.sqrt(2 * f - f * f)


def _m(phi: float, e: float) -> float:
    return math.cos(phi) / math.sqrt(1 - (e * math.sin(phi)) ** 2)


def _t(phi: float, e: float) -> float:
    sin_phi = math.sin(phi)
    return math.tan(math.pi / 4 - phi / 2) / (((1 - e * sin_phi) / (1 + e * sin_phi)) ** (e / 2))


def _phi_from_t(t: float, e: float) -> float:
    # Iterative solve. Start with spherical approximation.
    phi = math.pi / 2 - 2 * math.atan(t)
    for _ in range(20):
        sin_phi = math.sin(phi)
        phi_next = math.pi / 2 - 2 * math.atan(t * (((1 - e * sin_phi) / (1 + e * sin_phi)) ** (e / 2)))
        if abs(phi_next - phi) < 1e-12:
            return phi_next
        phi = phi_next
    return phi


def inverse_lcc(proj: Lcc2sp, x_m: float, y_m: float) -> tuple[float, float]:
    e = _eccentricity(proj.f_inv)

    m1 = _m(proj.phi1, e)
    m2 = _m(proj.phi2, e)
    t1 = _t(proj.phi1, e)
    t2 = _t(proj.phi2, e)
    t0 = _t(proj.phi0, e)

    n = (math.log(m1) - math.log(m2)) / (math.log(t1) - math.log(t2))
    f = m1 / (n * (t1**n))
    rho0 = proj.a * f * (t0**n)

    x = x_m - proj.fe
    y = rho0 - (y_m - proj.fn)
    rho = math.copysign(math.sqrt(x * x + y * y), n)
    theta = math.atan2(x, y)

    t = (rho / (proj.a * f)) ** (1 / n)
    phi = _phi_from_t(t, e)
    lam = proj.lam0 + theta / n

    return _rad(lam), _rad(phi)


def read_dbf(path: Path) -> list[dict[str, str]]:
    data = path.read_bytes()
    if len(data) < 32:
        raise ValueError("DBF too small")

    num_records = struct.unpack_from("<I", data, 4)[0]
    header_len = struct.unpack_from("<H", data, 8)[0]
    record_len = struct.unpack_from("<H", data, 10)[0]

    fields: list[tuple[str, str, int, int]] = []
    off = 32
    while off < header_len:
        if data[off] == 0x0D:
            off += 1
            break
        name_raw = data[off : off + 11]
        name = name_raw.split(b"\x00", 1)[0].decode("ascii", errors="ignore").strip()
        ftype = chr(data[off + 11])
        flen = data[off + 16]
        fdec = data[off + 17]
        fields.append((name, ftype, flen, fdec))
        off += 32

    records: list[dict[str, str]] = []
    rec_off = header_len
    for _ in range(num_records):
        if rec_off + record_len > len(data):
            break
        deleted = data[rec_off : rec_off + 1]
        rec_off += 1
        rec: dict[str, str] = {}
        for name, ftype, flen, _fdec in fields:
            raw = data[rec_off : rec_off + flen]
            rec_off += flen
            s = raw.decode("utf-8", errors="ignore").strip()
            rec[name] = s
        if deleted != b"*":
            records.append(rec)
    return records


def read_shp_polygons(path: Path) -> list[list[list[tuple[float, float]]]]:
    # Returns list of shapes; each shape = list of parts; each part = list of (x,y)
    data = path.read_bytes()
    if len(data) < 100:
        raise ValueError("SHP too small")
    off = 100
    shapes: list[list[list[tuple[float, float]]]] = []
    while off + 8 <= len(data):
        # record header (big endian)
        _rec_num = struct.unpack_from(">i", data, off)[0]
        content_len_words = struct.unpack_from(">i", data, off + 4)[0]
        off += 8
        content_len = content_len_words * 2
        if off + content_len > len(data):
            break

        shape_type = struct.unpack_from("<i", data, off)[0]
        if shape_type == 0:
            shapes.append([])
            off += content_len
            continue
        if shape_type != 5:
            raise ValueError(f"Unsupported shape type {shape_type} (expected Polygon=5)")

        # bbox: 4 doubles
        # xmin, ymin, xmax, ymax = struct.unpack_from("<4d", data, off + 4)
        num_parts = struct.unpack_from("<i", data, off + 36)[0]
        num_points = struct.unpack_from("<i", data, off + 40)[0]

        parts_idx_off = off + 44
        parts = list(struct.unpack_from(f"<{num_parts}i", data, parts_idx_off))
        points_off = parts_idx_off + 4 * num_parts

        pts: list[tuple[float, float]] = []
        for i in range(num_points):
            x, y = struct.unpack_from("<2d", data, points_off + i * 16)
            pts.append((x, y))

        shape_parts: list[list[tuple[float, float]]] = []
        for i in range(num_parts):
            start = parts[i]
            end = parts[i + 1] if i + 1 < num_parts else num_points
            ring = pts[start:end]
            if len(ring) >= 2 and ring[0] != ring[-1]:
                ring = ring + [ring[0]]
            shape_parts.append(ring)

        shapes.append(shape_parts)
        off += content_len
    return shapes


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    shp_dir = repo_root / "nyc_taxi_data" / "shapefiles" / "taxi_zones"
    shp_path = shp_dir / "taxi_zones.shp"
    dbf_path = shp_dir / "taxi_zones.dbf"
    out_path = repo_root / "frontend" / "public" / "taxi_zones.geojson"

    if not shp_path.exists():
        raise SystemExit(f"Missing {shp_path}")
    if not dbf_path.exists():
        raise SystemExit(f"Missing {dbf_path}")

    # Projection params from taxi_zones.prj (units Foot_US → meters)
    foot_us_to_m = 0.3048006096012192
    proj = Lcc2sp(
        a=6378137.0,
        f_inv=298.257222101,
        phi1=_deg(40.66666666666666),
        phi2=_deg(41.03333333333333),
        phi0=_deg(40.16666666666666),
        lam0=_deg(-74.0),
        fe=984250.0 * foot_us_to_m,
        fn=0.0 * foot_us_to_m,
    )

    attrs = read_dbf(dbf_path)
    shapes = read_shp_polygons(shp_path)
    if len(attrs) != len(shapes):
        # Still proceed; records may have been filtered by deletion flag.
        print(f"Warning: DBF records={len(attrs)} SHP shapes={len(shapes)} (will zip by index)")

    def get_field(rec: dict[str, str], *names: str) -> str | None:
        for n in names:
            for k, v in rec.items():
                if k.lower() == n.lower():
                    return v
        return None

    features = []
    for rec, parts in zip(attrs, shapes):
        loc = get_field(rec, "LocationID", "location_id", "LOCATIONID")
        zone = get_field(rec, "zone")
        borough = get_field(rec, "borough")
        service_zone = get_field(rec, "service_zone", "service", "subregion")
        if not loc:
            continue
        try:
            loc_id = int(float(loc))
        except ValueError:
            continue

        polygons: list[list[list[float]]] = []
        for ring in parts:
            if len(ring) < 4:
                continue
            coords: list[list[float]] = []
            for x_ft, y_ft in ring:
                lon, lat = inverse_lcc(proj, x_ft * foot_us_to_m, y_ft * foot_us_to_m)
                coords.append([lon, lat])
            polygons.append([coords])

        geom = {"type": "MultiPolygon", "coordinates": polygons}
        features.append(
            {
                "type": "Feature",
                "id": loc_id,
                "properties": {
                    "zone_id": loc_id,
                    "location_id": loc_id,
                    "zone": zone,
                    "borough": borough,
                    "service_zone": service_zone,
                },
                "geometry": geom,
            }
        )

    fc = {"type": "FeatureCollection", "features": features}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(fc), encoding="utf-8")
    print(f"Wrote {out_path} with {len(features)} features")


if __name__ == "__main__":
    main()

