class QuickbaseClient:
    """
    OOP Quickbase client that:
      1) fetches the field map once (FID -> label),
      2) paginates records,
      3) for each page, returns a 'batch' that includes the field map plus that page's rows,
         optionally flattened and renamed to labels.

    Usage example (inside your Flask service):
        qb = QuickbaseClient(cfg.QB_REALMID, cfg.QB_USER_TOKEN, cfg.QB_TABLEID, cfg.PAGE_SIZE)
        field_map = qb.get_field_map()
        for batch in qb.iter_batches(include_fields_each=True):
            # batch = {"page": n, "count": k, "fields": {...}, "records": [ {...}, ... ], "has_more": bool}
            gcs_writer.stream_jsonl(object_name, (r for r in batch["records"]))
    """

    QB_FIELDS_URL = "https://api.quickbase.com/v1/fields"
    QB_QUERY_URL  = "https://api.quickbase.com/v1/records/query"

    def __init__(self, realm: str, token: str, table_id: str, page_size: int = 1000, session=None):
        from requests import Session
        from requests.adapters import HTTPAdapter, Retry

        self.realm = realm
        self.token = token
        self.table_id = table_id
        self.page_size = int(page_size)

        # resilient HTTP session with retries
        self.session = session or Session()
        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=["GET", "POST"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.headers = {
            "QB-Realm-Hostname": self.realm,
            "Authorization": f"QB-USER-TOKEN {self.token}",
            "Content-Type": "application/json",
        }

        self._field_map_cache: dict[str, str] | None = None

    # ---------- Public API ----------

    def get_field_map(self) -> dict[str, str]:
        """
        Fetch and cache Quickbase fields for this table.
        Returns: { "<fid>": "<label or fieldName or fid>" }
        """
        if self._field_map_cache is not None:
            return self._field_map_cache

        r = self.session.get(self.QB_FIELDS_URL, headers=self.headers, params={"tableId": self.table_id}, timeout=90)
        r.raise_for_status()
        js = r.json()
        fields = js if isinstance(js, list) else js.get("fields", [])
        self._field_map_cache = {
            str(f.get("id")): (f.get("label") or f.get("fieldName") or str(f.get("id")))
            for f in fields
        }
        return self._field_map_cache

    def iter_batches(
        self,
        *,
        include_fields_each: bool = True,
        flatten_values: bool = True,
        rename_to_labels: bool = True,
        select_all_fields: bool = True,
        select: list[str] | None = None,
        max_pages: int | None = None,
        start_skip: int = 0,
    ):
        """
        Yields one batch (page) at a time.

        Each yield is a dict:
          {
            "page": <1-based page number>,
            "count": <rows in this page>,
            "fields": <field map dict> or None (controlled by include_fields_each),
            "records": [ {<col>: <value>, ...}, ... ],
            "has_more": <bool>
          }

        Args:
          include_fields_each: include the field map in every batch (useful if your writer expects it).
          flatten_values: convert QB cell objects {"value": ...} -> raw values.
          rename_to_labels: convert FIDs to human-readable labels using get_field_map().
          select_all_fields: if True, uses ["a"] which means ALL fields in Quickbase.
          select: if provided (list of FIDs), overrides 'select_all_fields'.
          max_pages: stop after N pages (for testing).
          start_skip: starting offset (for resuming).
        """
        import math
        payload = {
            "from": self.table_id,
            "select": (["a"] if select_all_fields else (select or [])),
            "options": {"top": self.page_size, "skip": int(start_skip)},
        }

        fmap = self.get_field_map() if rename_to_labels else None
        page_no = 0

        while True:
            resp = self.session.post(self.QB_QUERY_URL, headers=self.headers, json=payload, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            raw_batch = data.get("data", [])
            if not raw_batch:
                break

            # transform rows
            records = []
            for rec in raw_batch:
                row = rec
                if flatten_values:
                    row = self._flatten_one(row)
                if rename_to_labels and fmap is not None:
                    row = self._rename_fids(row, fmap)
                records.append(row)

            page_no += 1
            has_more = len(raw_batch) == self.page_size

            yield {
                "page": page_no,
                "count": len(records),
                "fields": (fmap if include_fields_each else None),
                "records": records,
                "has_more": has_more,
            }

            if max_pages and page_no >= max_pages:
                break

            payload["options"]["skip"] += self.page_size

    # ---------- Helpers ----------

    @staticmethod
    def _flatten_one(rec: dict) -> dict:
        """
        Quickbase record cells typically look like: {"6": {"value": ...}, "7": {"value": ...}}
        This flattens to: {"6": <value>, "7": <value>}
        """
        out = {}
        for k, v in rec.items():
            out[k] = v["value"] if isinstance(v, dict) and "value" in v else v
        return out

    @staticmethod
    def _rename_fids(rec: dict, fid2lbl: dict[str, str]) -> dict:
        """
        Rename FID keys to their label/fieldName when available.
        """
        return {fid2lbl.get(str(k), str(k)): v for k, v in rec.items()}
