# services/quickbase_client.py
from typing import List, Dict, Optional

class QuickbaseClient:
    """
    Minimal Quickbase client to fetch a specific page of records (no field map).
    Returns flattened rows ({"fid": {"value": ...}} -> {"fid": ...}).
    """

    QB_QUERY_URL = "https://api.quickbase.com/v1/records/query"

    def __init__(self, realm: str, token: str, table_id: str, session: Optional[object] = None):
        from requests import Session
        from requests.adapters import HTTPAdapter, Retry

        self.realm = realm
        self.token = token
        self.table_id = table_id

        self.session = session or Session()
        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=["POST"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

        self.headers = {
            "QB-Realm-Hostname": self.realm,
            "Authorization": f"QB-USER-TOKEN {self.token}",
            "Content-Type": "application/json",
        }

    def get_records(
        self,
        page_size: int,
        skip: int,
        *,
        flatten_values: bool = False,
        select_all_fields: bool = False,
        select: Optional[list] = [1,2,3,7],
        where: Optional[str] = None,
        timeout: int = 120,
    ) -> List[Dict]:
        """
        Fetch one page (window) of records.
        """
        payload = {
            "from": self.table_id,
            "select": (["a"] if select_all_fields else (select or [])),
            "options": {"top": int(page_size), "skip": int(skip)},
        }
        if where:
            payload["where"] = where

        resp = self.session.post(self.QB_QUERY_URL, headers=self.headers, json=payload, timeout=timeout)
        resp.raise_for_status()
        js = resp.json()
        raw_rows = js.get("data", []) or []

        print(raw_rows)
        
        if not flatten_values:
            return raw_rows
        
        return [self._flatten_one(row) for row in raw_rows]

    @staticmethod
    def _flatten_one(rec: Dict) -> Dict:
        return {k: (v.get("value") if isinstance(v, dict) and "value" in v else v) for k, v in rec.items()}
