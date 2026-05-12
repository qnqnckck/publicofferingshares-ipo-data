from __future__ import annotations

import os
import re
import time
from http.cookiejar import CookieJar
from typing import Any
from urllib.parse import quote, urlparse, urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener
from urllib.error import HTTPError


def _extract_login_token_from_html(raw_html: str) -> str:
    patterns = [
        r"""<input[^>]*name=['"]_token['"][^>]*value=['"]([^'"]+)['"]""",
        r"""<input[^>]*value=['"]([^'"]+)['"][^>]*name=['"]_token['"]""",
        r"""<meta[^>]*name=['"]csrf-token['"][^>]*content=['"]([^'"]+)['"]""",
        r"""<meta[^>]*content=['"]([^'"]+)['"][^>]*name=['"]csrf-token['"]""",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw_html, flags=re.IGNORECASE)
        token = match.group(1).strip() if match else ""
        if token:
            return token
    return ""


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
    token = _extract_login_token_from_html(login_page)
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


def fetch_finuts_authenticated_text(
    *,
    request_url: str,
    method: str = "GET",
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    login_target_url: str | None = None,
    referer: str | None = None,
) -> str:
    request_headers = {"User-Agent": "Mozilla/5.0", **(headers or {})}
    request = Request(
        request_url,
        data=data if method.upper() == "POST" else None,
        headers=request_headers,
        method=method.upper(),
    )
    opener = None
    try:
        opener = build_finuts_authenticated_opener(
            target_url=login_target_url or request_url,
        )
    except HTTPError as exc:
        if exc.code != 403:
            raise
    except Exception:
        pass
    if opener is not None:
        try:
            with opener.open(request, timeout=30) as response:
                return response.read().decode("utf-8", "ignore")
        except HTTPError as exc:
            if exc.code != 403:
                raise
        except Exception:
            pass
    return _fetch_finuts_text_via_browser(
        request_url=request_url,
        method=method,
        data=data,
        headers=request_headers,
        login_target_url=login_target_url or request_url,
        referer=referer,
    )


def _fetch_finuts_text_via_browser(
    *,
    request_url: str,
    method: str,
    data: bytes | None,
    headers: dict[str, str],
    login_target_url: str,
    referer: str | None,
) -> str:
    from playwright.sync_api import sync_playwright

    user_id = os.environ.get("FINUTS_ID", "").strip()
    user_password = os.environ.get("FINUTS_PASSWORD", "").strip()
    if not user_id or not user_password:
        raise RuntimeError("FINUTS_ID and FINUTS_PASSWORD are required for browser fallback.")

    parsed = urlparse(login_target_url)
    return_path = parsed.path or "/"
    if parsed.query:
        return_path = f"{return_path}?{parsed.query}"
    login_url = (
        f"{parsed.scheme}://{parsed.netloc}/html/user/login.php?"
        f"url={quote(return_path, safe='/?=&')}"
    )
    login_ajax_url = (
        f"{parsed.scheme}://{parsed.netloc}/html/task/user/ajaxMemberLoginCheck.php"
    )
    request_body = data.decode("utf-8", "ignore") if data else None

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        try:
            page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
            html = page.content()
            token = _extract_login_token_from_html(html)
            if not token:
                try:
                    token = (
                        page.locator('input[name="_token"]').first.get_attribute("value")
                        or ""
                    ).strip()
                except Exception:
                    token = ""
            if not token:
                try:
                    token = (
                        page.locator('meta[name="csrf-token"]').first.get_attribute(
                            "content"
                        )
                        or ""
                    ).strip()
                except Exception:
                    token = ""
            if not token:
                raise RuntimeError("Failed to extract Finuts login token in browser fallback.")
            login_payload = (
                f"user_id={quote(user_id)}&user_pwd={quote(user_password)}&save_id=&_token={quote(token)}"
            )
            login_result = page.evaluate(
                """async ({url, body, referer}) => {
                    const response = await fetch(url, {
                      method: 'POST',
                      headers: {
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'X-Requested-With': 'XMLHttpRequest',
                        'Referer': referer,
                      },
                      body,
                      credentials: 'include',
                    });
                    return await response.text();
                }""",
                {"url": login_ajax_url, "body": login_payload, "referer": login_url},
            )
            if '"S"' not in login_result and str(login_result).strip() != "S":
                raise RuntimeError("Finuts browser login failed.")
            return page.evaluate(
                """async ({url, method, body, headers, referer}) => {
                    const response = await fetch(url, {
                      method,
                      headers: {
                        ...headers,
                        ...(referer ? {'Referer': referer} : {}),
                      },
                      body,
                      credentials: 'include',
                    });
                    return await response.text();
                }""",
                {
                    "url": request_url,
                    "method": method.upper(),
                    "body": request_body,
                    "headers": headers,
                    "referer": referer or login_target_url,
                },
            )
        finally:
            browser.close()
