import re
import xml.etree.ElementTree as ET
from math import inf

SVG_NS = "http://www.w3.org/2000/svg"
ET.register_namespace("", SVG_NS)

COMMAND_RE = re.compile(r"([MLZ])([^MLZ]*)")

def parse_path_d(d):
    subpaths = []
    current = []
    tokens = COMMAND_RE.findall(d)

    for cmd, args in tokens:
        cmd = cmd.upper()
        if cmd in ("M", "L"):
            coords = re.split(r"[ ,]+", args.strip())
            for i in range(0, len(coords) - 1, 2):
                if coords[i] and coords[i+1]:
                    x = float(coords[i])
                    y = float(coords[i+1])
                    if cmd == "M" and current:
                        subpaths.append(current)
                        current = []
                    current.append((x, y))
        elif cmd == "Z":
            if current:
                subpaths.append(current)
                current = []
    if current:
        subpaths.append(current)
    return subpaths


def format_path(subpaths):
    parts = []
    for ring in subpaths:
        if not ring:
            continue
        x0, y0 = ring[0]
        parts.append(f"M{x0:.3f},{y0:.3f}")
        for x, y in ring[1:]:
            parts.append(f"L{x:.3f},{y:.3f}")
        parts.append("Z")
    return " ".join(parts)


def get_projection_params(root):
    attrs = root.attrib
    return (
        float(attrs["data-lon-min"]),
        float(attrs["data-lon-max"]),
        float(attrs["data-lat-min"]),
        float(attrs["data-lat-max"]),
        float(attrs["data-margin"]),
        float(attrs["data-scale"]),
    )


def svg_xy_to_lonlat(x, y, lon_min, lat_max, margin, scale):
    lon = (x - margin) / scale + lon_min
    lat = lat_max - (y - margin) / scale
    return lon, lat


def lonlat_to_svg_xy(lon, lat, lon_min, lat_max, margin, scale):
    x = margin + (lon - lon_min) * scale
    y = margin + (lat_max - lat) * scale
    return x, y


def extract_lonlat_paths(root):
    lon_min, lon_max, lat_min, lat_max, margin, scale = get_projection_params(root)
    paths = []
    ns = {"svg": SVG_NS}

    for el in root.findall(".//svg:path", ns):
        d = el.get("d")
        if not d:
            continue
        xy_paths = parse_path_d(d)
        ll_paths = []
        for ring in xy_paths:
            ll_ring = [
                svg_xy_to_lonlat(x, y, lon_min, lat_max, margin, scale)
                for x, y in ring
            ]
            ll_paths.append(ll_ring)
        paths.append((el, ll_paths))
    return paths


def merge_svgs_geo(svg_paths, width=1200, margin=10.0):
    """
    Return a merged SVG root element, without writing to disk.
    Useful for importing as a library.
    """
    if len(svg_paths) < 2:
        raise ValueError("Need at least two SVG files.")

    trees = [ET.parse(p) for p in svg_paths]
    roots = [t.getroot() for t in trees]

    # Collect lon/lat from all paths
    all_paths = []
    for root in roots:
        all_paths.extend(extract_lonlat_paths(root))

    # Compute global bounds
    lon_min_g = inf
    lon_max_g = -inf
    lat_min_g = inf
    lat_max_g = -inf

    for _, rings in all_paths:
        for ring in rings:
            for lon, lat in ring:
                lon_min_g = min(lon_min_g, lon)
                lon_max_g = max(lon_max_g, lon)
                lat_min_g = min(lat_min_g, lat)
                lat_max_g = max(lat_max_g, lat)

    # Global projection
    lon_span = lon_max_g - lon_min_g
    lat_span = lat_max_g - lat_min_g
    scale = (width - 2 * margin) / lon_span
    height = lat_span * scale + 2 * margin

    # New root SVG
    new_root = ET.Element(
        f"{{{SVG_NS}}}svg",
        {
            "width": str(width),
            "height": str(height),
            "viewBox": f"0 0 {width} {height}",
            "data-lon-min": str(lon_min_g),
            "data-lon-max": str(lon_max_g),
            "data-lat-min": str(lat_min_g),
            "data-lat-max": str(lat_max_g),
            "data-margin": str(margin),
            "data-scale": str(scale),
        },
    )

    # Copy <defs> from first file
    for child in list(roots[0]):
        if child.tag.endswith("defs"):
            new_root.append(child)

    # Add all paths in global projection
    for src_el, rings in all_paths:
        new_subpaths = []
        for ring in rings:
            new_ring = [
                lonlat_to_svg_xy(lon, lat, lon_min_g, lat_max_g, margin, scale)
                for lon, lat in ring
            ]
            new_subpaths.append(new_ring)

        new_el = ET.Element(f"{{{SVG_NS}}}path")
        for k, v in src_el.attrib.items():
            if k not in ("d", "transform"):
                new_el.set(k, v)
        new_el.set("d", format_path(new_subpaths))

        new_root.append(new_el)

    return new_root


def write_svg(root, path):
    """Write root element to an SVG file."""
    tree = ET.ElementTree(root)
    tree.write(path, encoding="utf-8", xml_declaration=True)
