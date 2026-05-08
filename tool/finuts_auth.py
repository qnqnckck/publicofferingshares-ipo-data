from __future__ import annotations

import os
import re
import time
from http.cookiejar import CookieJar
from typing import Any
from urllib.parse import quote, urlparse, urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener


def build_finuts_authenticated_opener(
    *,
    target_url: str,
    finuts_id: str | None = None,
    finuts_password: str | None = None,
) -> Any | None:
    user_id = (finuts_id or os.environ.get("FINUTS_ID", "")).strip()
    user_password = (finuts_password or os.environ.get("FINUTS_PASSWORD", "")).strip()
    if not user_id or not user_password:
        return None

    parsed = urlparse(target_url)
    return_path = parsed.path or "/"
    if parsed.query:
        return_path = f"{return_path}?{parsed.query}"
    login_url = (
        f"{parsed.scheme}://{parsed.netloc}/html/user/login.php?"
        f"url={quote(return_path, safe='/?=&')}"
    )

    cookie_jar = CookieJar()
    opener = build_opener(HTTPCookieProcessor(cookie_jar))
    opener.addheaders = [("User-Agent", "Mozilla/5.0")]

    def _open_with_retry(request: str | Request, *, timeout: int = 30) -> str:
        attempts = 3
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return opener.open(request, timeout=timeout).read().decode(
                    "utf-8",
                    errors="ignore",
                )
            except Exception as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                time.sleep(1.0 * attempt)
        if last_error is not None:
            raise last_error
        return ""

    login_page = _open_with_retry(login_url)
    token_match = re.search(
        r'<input[^>]+name="_token"[^>]+value="([^"]+)"',
        login_page,
        flags=re.IGNORECASE,
    )
    token = token_match.group(1).strip() if token_match else ""
    if not token:
        return None

    login_payload = urlencode(
        {
            "user_id": user_id,
            "user_pwd": user_password,
            "save_id": "",
            "_token": token,
        }
    ).encode()
    login_request = Request(
        f"{parsed.scheme}://{parsed.netloc}/html/task/user/ajaxMemberLoginCheck.php",
        data=login_payload,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": login_url,
        },
    )
    login_response = _open_with_retry(login_request)
    if '"S"' not in login_response and "S" != login_response.strip():
        return None
    return opener
