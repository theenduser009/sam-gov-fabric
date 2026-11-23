def write_json_file(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{uuid4().hex}.part")
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def signed_months(start, end):
    while end >= start:
        low = max(start, end.replace(day=1))
        yield low, end
        if low == start:
            break
        end = low - timedelta(days=1)


def split_window(kind, start, end):
    if start == end:
        raise RuntimeError(f"{kind} {start} exceeded SAM.gov's search limit for one day.")
    middle = start + (end - start) // 2
    return [(kind, middle + timedelta(days=1), end), (kind, start, middle)]


def land_page(client, root, run, kind, start, end):
    window = f"signed_from={start}/signed_to={end}"
    marker = root / "_checkpoints/json_search" / window / f"{kind.lower()}.json"
    previous = json.loads(marker.read_text(encoding="utf-8")) if marker.is_file() else {}
    if previous.get("complete"):
        return []
    if previous.get("split"):
        return split_window(kind, start, end)

    page = previous.get("next_page", 0)
    offset = previous.get("next_offset", page)
    records, total = client.page(kind, start, end, offset)
    if page and total != previous["total"]:
        raise RuntimeError(
            f"{kind} {start} through {end}: the match count changed during paging. "
            "The saved pages are available. Use a different signed-date range for a new load."
        )
    if total > MAX_SEARCH_RECORDS:
        children = split_window(kind, start, end)
        write_json_file(marker, {"split": True})
        return children

    fingerprint = hashlib.sha256(
        json.dumps(records, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    if records and fingerprint == previous.get("last_page_sha256"):
        if page == 1 and offset == 1:
            # Some responses treat offset 1 as the first page when offset is omitted.
            write_json_file(marker, {**previous, "next_offset": 2})
            return [(kind, start, end)]
        raise RuntimeError(f"{kind}: SAM.gov repeated the previous page. Saved progress was kept.")

    expected = min(PAGE_SIZE, max(0, total - page * PAGE_SIZE))
    if len(records) != expected:
        raise RuntimeError(
            f"{kind} page {page} contained {len(records)} records. "
            f"The reported total implies {expected}. Saved progress was kept."
        )

    if records:
        target = run / f"record_type={kind.lower()}" / window / f"page-{page:04d}.json"
        write_json_file(target, {
            "awardSummary": records,
            "totalRecords": total,
            "offset": offset,
            "limit": PAGE_SIZE,
        })
        print(f"Saved {len(records):,} {kind} records to {target.relative_to(LAKEHOUSE_FILES).as_posix()}")

    complete = page * PAGE_SIZE + len(records) == total
    write_json_file(marker, {
        "next_page": page + 1,
        "next_offset": offset + 1,
        "total": total,
        "complete": complete,
        "last_page_sha256": fingerprint,
    })
    if complete:
        if total == 0:
            print(f"No {kind} records for {start} through {end}.")
        return []
    return [(kind, start, end)]
