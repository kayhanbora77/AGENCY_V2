import duckdb
import uuid
import pandas as pd


DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "MMT_RAW"


# ============================================================================
# Helpers
# ============================================================================


def is_null(v):
    """True for None/NaN/NaT/empty-string airport values."""
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def date_gap_days(prev_date, curr_date):
    """Return integer day gap, or None if either date is missing/unparseable."""
    if prev_date is None or curr_date is None:
        return None
    try:
        if pd.isna(prev_date) or pd.isna(curr_date):
            return None
        return (curr_date - prev_date).days
    except Exception:
        return None


# ============================================================================
# Step 1: cluster rows of the same BookingId by date proximity (<=1 day gap)
# ============================================================================


def cluster_by_date(rows, max_gap_days=1):
    """rows must already be sorted by DepartureDate (Nones last)."""
    if not rows:
        return []
    clusters = []
    current = [rows[0]]

    for prev, curr in zip(rows, rows[1:]):
        gap = date_gap_days(prev.get("DepartureDate"), curr.get("DepartureDate"))
        if gap is not None and gap <= max_gap_days:
            current.append(curr)
        else:
            clusters.append(current)
            current = [curr]

    clusters.append(current)
    return clusters


# ============================================================================
# Step 2: within a date, build the airport-match graph and turn it into one
# or more ordered "segments" (a segment = a chain of one or more legs flown
# on the same DepartureDate that connect to each other).
#
# IMPORTANT: edges are built ONLY among rows sharing the same DepartureDate.
# Building edges across the whole cluster (all dates at once) is what caused
# the original bug: a hub airport used twice (e.g. IST on both the outbound
# and the return leg of a round trip) creates spurious cross-day edges that
# corrupt the in-degree/out-degree pattern the cycle/path detection relies
# on. Restricting to same-date rows keeps each day's small subgraph clean;
# cross-day stitching is handled separately and more carefully below.
# ============================================================================


def _build_edges(cluster, idxs):
    edges = set()
    for i in idxs:
        for j in idxs:
            if i == j:
                continue
            if cluster[i]["NewArrAirport"] == cluster[j]["NewDepAirport"]:
                edges.add((i, j))
    return edges


def _order_component_as_chains(cluster, component, edges):
    """Turn one connected component (rows linked by airport matches, all on
    the same date) into one or more ordered chains of row-indices."""

    if len(component) == 2:
        a, b = component
        mutual = (a, b) in edges and (b, a) in edges
        if mutual:
            # A->B and B->A on the same day: a same-day round trip, not a
            # real connection. Keep as two standalone single-leg chains.
            return [[a], [b]]
        first, second = (a, b) if (a, b) in edges else (b, a)
        return [[first, second]]

    comp_set = set(component)
    out_edges = {i: [] for i in component}
    in_degree = {i: 0 for i in component}
    for i, j in edges:
        if i in comp_set and j in comp_set:
            out_edges[i].append(j)
            in_degree[j] += 1
    out_degree = {i: len(out_edges[i]) for i in component}

    is_simple_cycle = all(out_degree[i] == 1 for i in component) and all(
        in_degree[i] == 1 for i in component
    )

    if is_simple_cycle:
        # All same date here, so there's no meaningful "backward in time"
        # seam to cut at -- just walk it once as a single chain.
        ordered = [component[0]]
        current = component[0]
        for _ in range(len(component) - 1):
            current = out_edges[current][0]
            ordered.append(current)
        return [ordered]

    start_candidates = [i for i in component if in_degree[i] == 0]
    if len(start_candidates) == 1 and all(d <= 1 for d in out_degree.values()):
        walk = [start_candidates[0]]
        seen = {start_candidates[0]}
        current = start_candidates[0]
        ok = True
        while out_edges[current]:
            current = out_edges[current][0]
            if current in seen or len(walk) > len(component):
                ok = False
                break
            seen.add(current)
            walk.append(current)
        if ok and len(walk) == len(component):
            return [walk]

    # Dirty/branching data for this date -- fall back to a stable order
    # rather than guessing; this is a rare edge case worth flagging if it
    # comes up often in practice.
    return [list(component)]


def chain_within_date(cluster, idxs):
    """Given all non-null row indices that share one DepartureDate, return a
    list of segments, each: {"order": [...], "first_airport":, "last_airport":}.
    """
    edges = _build_edges(cluster, idxs)
    adj = {i: set() for i in idxs}
    for i, j in edges:
        adj[i].add(j)
        adj[j].add(i)

    visited = set()
    segments = []
    for i in idxs:
        if i in visited:
            continue
        stack, component = [i], []
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            component.append(node)
            stack.extend(nb for nb in adj[node] if nb not in visited)

        if len(component) == 1:
            idx0 = component[0]
            segments.append(
                {
                    "order": [idx0],
                    "first_airport": cluster[idx0]["NewDepAirport"],
                    "last_airport": cluster[idx0]["NewArrAirport"],
                }
            )
        else:
            for chain in _order_component_as_chains(cluster, component, edges):
                segments.append(
                    {
                        "order": chain,
                        "first_airport": cluster[chain[0]]["NewDepAirport"],
                        "last_airport": cluster[chain[-1]]["NewArrAirport"],
                    }
                )

    return segments


# ============================================================================
# Step 3: stitch same-date segments together across consecutive dates into
# full connections, EXCEPT when doing so would close a loop back to the
# chain's true origin -- that's the signature of a round trip ending, which
# must stay as a separate connection rather than one long chain.
# ============================================================================


def finalize_chain(cluster, indices, bridged_in=False):
    conn_id = uuid.uuid4()
    for leg_no, idx in enumerate(indices, start=1):
        cluster[idx]["ConnectionId"] = conn_id
        cluster[idx]["LegNo"] = leg_no
    # A single-leg finalized chain is only an airport problem if it NEVER
    # matched any other row's airport -- i.e. it didn't bridge in from a
    # previous chain at all. If it bridged in but the loop closed (e.g. the
    # final BOM->HYD leg after HYD->BOM->LHR->BOM), that row matched its
    # neighbor's airport just fine and ended up alone only because the
    # round trip completed -- that's NOT a problem.
    if len(indices) == 1 and not bridged_in:
        cluster[indices[0]]["IsAirportProblem"] = True


def stitch_segments(cluster, segments):
    """
    Stitch same-date segments across consecutive dates into full connections.

    A loop "closes" -- i.e. forces a cut, ending the current connection and
    starting a fresh one -- as soon as a segment's last_airport revisits ANY
    airport already on the currently-open chain's path. That's what
    distinguishes "outbound: HYD->BOM->LHR" (new airports the whole way) from
    "return: LHR->BOM->HYD" (BOM and then HYD are both repeats) -- the
    turnaround happens right when the path is about to double back over
    itself, not only when it makes it all the way back to the origin.

    A new chain is also started whenever a segment simply doesn't bridge at
    all (its first_airport doesn't match the previous chain's last_airport).
    That case is tracked separately (bridged_in=False) from a loop-closing
    cut (bridged_in=True), so finalize_chain can tell a genuinely orphaned
    single-leg row (e.g. MEL->SYD with no shared airport anywhere else in
    the booking) apart from a single-leg row that's alone only because its
    round trip just closed.
    """
    open_chain = None

    def new_chain(seg, bridged_in=False):
        return {
            "indices": list(seg["order"]),
            "first_airport": seg["first_airport"],
            "last_airport": seg["last_airport"],
            "waypoints": {seg["first_airport"], seg["last_airport"]},
            "bridged_in": bridged_in,
        }

    for seg in segments:
        if open_chain is None:
            open_chain = new_chain(seg, bridged_in=False)
            continue

        bridges = (
            open_chain["last_airport"] is not None
            and seg["first_airport"] is not None
            and open_chain["last_airport"] == seg["first_airport"]
        )
        closes_loop = bridges and seg["last_airport"] in open_chain["waypoints"]

        if bridges and not closes_loop:
            open_chain["indices"].extend(seg["order"])
            open_chain["last_airport"] = seg["last_airport"]
            open_chain["waypoints"].add(seg["last_airport"])
        else:
            finalize_chain(cluster, open_chain["indices"], open_chain["bridged_in"])
            # bridges=True here means we cut because of closes_loop, so the
            # new chain DID bridge in. bridges=False means no airport match
            # at all -- a fresh, so-far-unconnected start.
            open_chain = new_chain(seg, bridged_in=bridges)

    if open_chain is not None:
        finalize_chain(cluster, open_chain["indices"], open_chain["bridged_in"])


# ============================================================================
# Step 4: orchestrate a single cluster
# ============================================================================


def process_cluster(cluster):
    n = len(cluster)

    for r in cluster:
        r.setdefault("IsAirportProblem", False)
        r.setdefault("ConnectionId", None)
        r.setdefault("LegNo", None)

    if n == 1:
        cluster[0]["ConnectionId"] = uuid.uuid4()
        cluster[0]["LegNo"] = 1
        return cluster

    null_mask = [
        is_null(r.get("NewDepAirport")) or is_null(r.get("NewArrAirport"))
        for r in cluster
    ]

    # Null-airport rows: flag as problems, isolate, don't use them in edges
    for i in range(n):
        if null_mask[i]:
            cluster[i]["IsAirportProblem"] = True
            cluster[i]["ConnectionId"] = uuid.uuid4()
            cluster[i]["LegNo"] = 1

    non_null_idx = [i for i in range(n) if not null_mask[i]]
    if not non_null_idx:
        return cluster

    # Group non-null rows by DepartureDate, preserving chronological order
    # (cluster is already sorted by DepartureDate coming in).
    groups_by_date = {}
    order_of_dates = []
    for idx in non_null_idx:
        d = cluster[idx]["DepartureDate"]
        if d not in groups_by_date:
            groups_by_date[d] = []
            order_of_dates.append(d)
        groups_by_date[d].append(idx)

    all_segments = []
    for d in order_of_dates:
        all_segments.extend(chain_within_date(cluster, groups_by_date[d]))

    stitch_segments(cluster, all_segments)

    return cluster


# ============================================================================
# Step 5: orchestrate per BookingId
# ============================================================================


def detect_connections(df):
    df_list = df.to_dict(orient="records")

    grouped = {}
    for r in df_list:
        grouped.setdefault(r.get("BookingId"), []).append(r)

    result = []
    for booking_id, rows in grouped.items():
        rows.sort(key=lambda r: (r["DepartureDate"] is None, r["DepartureDate"]))
        for cluster in cluster_by_date(rows):
            result.extend(process_cluster(cluster))

    return result


# ============================================================================
# Database I/O
# ============================================================================


def fetch_data(conn, table, limit=None):
    query = f"""
        SELECT * FROM {table}
        ORDER BY BOOKINGID, DEPARTUREDATE
    """
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query).fetchdf()


def persist_connections(conn, df_list, table=SOURCE_TABLE):
    """Bulk-write results back via a join-update instead of row-by-row
    executemany, which is far too slow at tens-of-millions-of-rows scale."""

    conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS ConnectionId UUID;")
    conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS LegNo INTEGER;")
    conn.execute(
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS IsAirportProblem BOOLEAN;"
    )

    if not df_list:
        print("Nothing to persist.")
        return

    results_df = pd.DataFrame(
        [
            {
                "Id": r["Id"],
                "ConnectionId": str(r.get("ConnectionId"))
                if r.get("ConnectionId")
                else None,
                "LegNo": r.get("LegNo"),
                "IsAirportProblem": bool(r.get("IsAirportProblem", False)),
            }
            for r in df_list
        ]
    )

    conn.register("results_df", results_df)

    conn.execute(
        f"""
        UPDATE {table} AS t
        SET
            ConnectionId = r.ConnectionId,
            LegNo = r.LegNo,
            IsAirportProblem = r.IsAirportProblem
        FROM results_df AS r
        WHERE t.Id = r.Id
        """
    )

    conn.unregister("results_df")
    print(f"Persisted {len(results_df)} records.")


# ============================================================================
# Main
# ============================================================================


def main():
    with duckdb.connect(DB_PATH) as conn:
        df = fetch_data(conn, SOURCE_TABLE)
        result_list = detect_connections(df)

        persist_connections(conn, result_list)


if __name__ == "__main__":
    main()
