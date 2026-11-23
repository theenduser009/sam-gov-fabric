def ingest():
    key = sam_api_key.strip()
    if not key:
        raise ValueError("Enter the SAM.gov key in sam_api_key in the second code cell.")
    if not LAKEHOUSE_FILES.is_dir():
        raise RuntimeError("Attach the destination Lakehouse as this notebook's default Lakehouse.")

    start = date.fromisoformat(signed_from.strip())
    end = date.fromisoformat(signed_to.strip())
    request_limit = int(max_api_requests)
    if start > end or request_limit < 1:
        raise ValueError("Check the signed-date range and request budget in the first code cell.")

    now = datetime.now(timezone.utc)
    root = LAKEHOUSE_FILES / "raw/sam/contract-awards"
    run = root / f"ingested_date={now:%Y-%m-%d}" / f"run={now:%Y%m%dT%H%M%SZ}-{uuid4().hex[:12]}" / "source=json_search"

    status = "complete"
    with requests.Session() as session:
        client = SamSearch(session, key, request_limit)
        try:
            for low, high in signed_months(start, end):
                pending = deque((kind, low, high) for kind in ("Award", "IDV"))
                while pending:
                    kind, window_start, window_end = pending.popleft()
                    pending.extend(land_page(client, root, run, kind, window_start, window_end))
        except LoadPaused as paused:
            status = "paused"
            print(paused)

    receipt = {
        "status": status,
        "signed_from": str(start),
        "signed_to": str(end),
        "ingested_utc": now.isoformat(),
        "api_requests": request_limit - client.remaining,
    }
    write_json_file(run / "_RUN.json", receipt)
    if status == "complete":
        write_json_file(run / "_SUCCESS.json", receipt)

    print(f"Run {status}. Lakehouse folder: Files/{run.relative_to(LAKEHOUSE_FILES).as_posix()}")


ingest()
