#!/usr/bin/env python3
"""Post IPO promotion content to DCInside galleries.

The admin server passes title/body via temporary files so Korean text and long
posts do not have to travel through command-line arguments.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from urllib.parse import quote


def read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Post promotion content to DCInside.")
    parser.add_argument("--title-file", required=True)
    parser.add_argument("--body-file", required=True)
    parser.add_argument("--galleries", required=True)
    parser.add_argument("--image")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def first_visible(page, selectors: list[str], timeout: int = 1500):
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=timeout)
            return locator
        except Exception:
            continue
    return None


def fill_first(page, selectors: list[str], value: str, label: str) -> None:
    locator = first_visible(page, selectors)
    if locator is None:
        raise RuntimeError(f"Could not find {label} field.")
    locator.fill(value)


def click_first(page, selectors: list[str], label: str) -> None:
    locator = first_visible(page, selectors)
    if locator is None:
        raise RuntimeError(f"Could not find {label} button.")
    locator.click()


def page_snapshot_path(gallery: str, suffix: str) -> Path:
    snapshot_dir = Path(os.environ.get("IPO_PROMOTION_DEBUG_DIR", "tmp/promotion-debug"))
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    safe_gallery = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in gallery)

    return snapshot_dir / f"{int(time.time())}-{safe_gallery}-{suffix}.png"


def visible_text(page) -> str:
    try:
        return page.locator("body").inner_text(timeout=2000)
    except Exception:
        return ""


def login_if_needed(page, user_id: str, password: str, return_url: str) -> None:
    page.goto(return_url, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(1500)

    if "sign.dcinside.com" not in page.url and "login" not in page.url.lower():
        return

    login_url = "https://sign.dcinside.com/login?s_url=" + quote(return_url, safe="")
    page.goto(login_url, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(1500)

    fill_first(
        page,
        [
            'input[name="user_id"]',
            'input[name="id"]',
            'input[id*="user"]',
            'input[id*="id"]',
            'input[type="text"]',
        ],
        user_id,
        "login id",
    )
    fill_first(
        page,
        [
            'input[name="pw"]',
            'input[name="password"]',
            'input[id*="pw"]',
            'input[id*="pass"]',
            'input[type="password"]',
        ],
        password,
        "login password",
    )
    click_first(
        page,
        [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("로그인")',
            'a:has-text("로그인")',
        ],
        "login",
    )
    page.wait_for_load_state("domcontentloaded", timeout=45000)
    page.wait_for_timeout(2500)

    if "sign.dcinside.com" in page.url or "login" in page.url.lower():
        screenshot_path = page_snapshot_path("login", "failed")
        page.screenshot(path=str(screenshot_path), full_page=True)
        raise RuntimeError(f"DCInside login did not complete. screenshot={screenshot_path}")


def fill_body(page, body: str) -> None:
    textarea = first_visible(
        page,
        [
            'textarea[name="memo"]',
            'textarea[name="content"]',
            'textarea[name="body"]',
            "textarea",
        ],
        timeout=1200,
    )
    if textarea is not None:
        textarea.fill(body)
        return

    editor = first_visible(
        page,
        [
            '[contenteditable="true"]',
            ".note-editable",
            "#tx_canvas_wysiwyg",
            "iframe",
        ],
        timeout=1200,
    )
    if editor is None:
        raise RuntimeError("Could not find post body editor.")

    tag_name = editor.evaluate("el => el.tagName.toLowerCase()")
    if tag_name == "iframe":
        frame = editor.element_handle().content_frame()
        if frame is None:
            raise RuntimeError("Could not access post body iframe.")
        target = first_visible(
            frame,
            ["body[contenteditable='true']", "body", "[contenteditable='true']"],
            timeout=1500,
        )
        if target is None:
            raise RuntimeError("Could not find iframe editor body.")
        target.fill(body)
        return

    editor.fill(body)


def upload_image_if_present(page, image_path: str | None) -> None:
    if not image_path:
        return

    path = Path(image_path)
    if not path.exists():
        raise RuntimeError(f"Image file does not exist: {path}")

    file_input = page.locator('input[type="file"]').first
    try:
        file_input.set_input_files(str(path), timeout=3000)
        print(f"attached image: {path.name}")
    except Exception:
        print("image upload field was not found; continuing without image")


def dcinside_write_url(gallery: str) -> str:
    regular_galleries = {"gongmozoo"}
    board_path = "board" if gallery in regular_galleries else "mgallery/board"

    return f"https://gall.dcinside.com/{board_path}/write/?id={quote(gallery)}"


def assert_post_submitted(page, gallery: str, write_url: str, dialog_messages: list[str]) -> None:
    page.wait_for_timeout(3000)

    if dialog_messages:
        screenshot_path = page_snapshot_path(gallery, "dialog")
        page.screenshot(path=str(screenshot_path), full_page=True)
        raise RuntimeError(
            f"DCInside blocked submit with dialog: {' / '.join(dialog_messages)}. "
            f"screenshot={screenshot_path}"
        )

    current_url = page.url
    body_text = visible_text(page)
    blocking_keywords = [
        "자동등록방지",
        "보안코드",
        "캡차",
        "captcha",
        "권한",
        "로그인",
        "비밀번호",
        "도배",
        "제한",
        "차단",
        "금지",
    ]
    found_keyword = next(
        (keyword for keyword in blocking_keywords if keyword.lower() in body_text.lower()),
        "",
    )

    if "/write" in current_url or current_url == write_url or found_keyword:
        screenshot_path = page_snapshot_path(gallery, "not-submitted")
        page.screenshot(path=str(screenshot_path), full_page=True)
        detail = f" keyword={found_keyword}" if found_keyword else ""
        raise RuntimeError(
            f"DCInside submit was not confirmed. url={current_url}{detail}. "
            f"screenshot={screenshot_path}"
        )


def post_gallery(page, gallery: str, title: str, body: str, image_path: str | None) -> None:
    write_url = dcinside_write_url(gallery)
    user_id = os.environ.get("DCINSIDE_ID", "").strip()
    password = os.environ.get("DCINSIDE_PASSWORD", "").strip()
    if not user_id or not password:
        raise RuntimeError("DCINSIDE_ID and DCINSIDE_PASSWORD are required.")

    print(f"opening DCInside gallery: {gallery}")
    dialog_messages: list[str] = []
    page.on("dialog", lambda dialog: (
        dialog_messages.append(dialog.message),
        dialog.accept(),
    ))
    login_if_needed(page, user_id, password, write_url)

    if "write" not in page.url:
        page.goto(write_url, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(1500)

    fill_first(
        page,
        [
            'input[name="subject"]',
            'input[name="title"]',
            'input[id*="subject"]',
            'input[id*="title"]',
            'input[type="text"]',
        ],
        title,
        "post title",
    )
    fill_body(page, body)
    upload_image_if_present(page, image_path)

    click_first(
        page,
        [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("등록")',
            'a:has-text("등록")',
            'button:has-text("작성")',
            'a:has-text("작성")',
        ],
        "submit",
    )
    assert_post_submitted(page, gallery, write_url, dialog_messages)
    print(f"submitted DCInside gallery: {gallery}")


def run_live(title: str, body: str, galleries: list[str], image_path: str | None) -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is required. Run: python -m pip install playwright && "
            "python -m playwright install chromium"
        ) from exc

    headless = env_bool("DCINSIDE_HEADLESS", False)
    slow_mo = int(os.environ.get("DCINSIDE_SLOW_MO_MS", "100"))

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless, slow_mo=slow_mo)
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()
        try:
            for gallery in galleries:
                post_gallery(page, gallery, title, body, image_path)
        finally:
            browser.close()

    return 0


def main() -> int:
    args = parse_args()
    title = read_text(args.title_file)
    body = read_text(args.body_file)
    galleries = [item.strip() for item in args.galleries.split(",") if item.strip()]

    if not galleries:
        raise RuntimeError("At least one gallery is required.")

    if args.dry_run:
        print("dry-run: DCInside post script loaded")
        print(f"dry-run: galleries={','.join(galleries)}")
        print(f"dry-run: titleLength={len(title)} bodyLength={len(body)}")
        print(f"dry-run: image={'yes' if args.image else 'no'}")
        return 0

    return run_live(title, body, galleries, args.image)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERR {exc}", file=sys.stderr)
        raise SystemExit(1)
