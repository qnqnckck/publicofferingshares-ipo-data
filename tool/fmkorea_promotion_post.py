#!/usr/bin/env python3
"""Post IPO promotion content to FM Korea boards."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

SCRIPT_VERSION = "fmkorea-post-v1"


def read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Post promotion content to FM Korea.")
    parser.add_argument("--title-file", required=True)
    parser.add_argument("--body-file", required=True)
    parser.add_argument("--board", default="stock")
    parser.add_argument("--image")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name, "").strip().lower()
    if not value:
        return default

    return value in {"1", "true", "yes", "y", "on"}


def page_snapshot_path(board: str, suffix: str) -> Path:
    snapshot_dir = Path(os.environ.get("IPO_PROMOTION_DEBUG_DIR", "tmp/promotion-debug"))
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    safe_board = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in board)

    return snapshot_dir / f"{int(time.time())}-fmkorea-{safe_board}-{suffix}.png"


def save_debug_snapshot(page, board: str, suffix: str) -> Path:
    screenshot_path = page_snapshot_path(board, suffix)
    page.screenshot(path=str(screenshot_path), full_page=True)
    screenshot_path.with_suffix(".html").write_text(page.content(), encoding="utf-8")

    return screenshot_path


def visible_text(page) -> str:
    try:
        return page.locator("body").inner_text(timeout=2000)
    except Exception:
        return ""


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
    errors: list[str] = []
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=2000)
            input_type = (locator.get_attribute("type", timeout=500) or "").lower()
            if input_type in {"checkbox", "radio", "hidden", "submit", "button", "file"}:
                errors.append(f"{selector}: skipped input type {input_type}")
                continue
            locator.fill(value)
            return
        except Exception as exc:
            errors.append(f"{selector}: {exc}")

    screenshot_path = save_debug_snapshot(page, "field", f"{label.replace(' ', '-')}-missing")
    raise RuntimeError(
        f"Could not find FM Korea {label} field. screenshot={screenshot_path}. "
        f"attempts={' | '.join(errors[:8])}"
    )


def board_url(board: str) -> str:
    return f"https://www.fmkorea.com/{quote(board)}"


def write_url(board: str) -> str:
    return f"https://www.fmkorea.com/index.php?mid={quote(board)}&act=dispBoardWrite"


def is_logged_in(page) -> bool:
    text = visible_text(page)
    if "로그아웃" in text or "쪽지함" in text or "내 정보" in text:
        return True

    for selector in [
        'a[href*="logout"]',
        'a[href*="dispMemberLogout"]',
        'a:has-text("로그아웃")',
    ]:
        try:
            if page.locator(selector).first.is_visible(timeout=700):
                return True
        except Exception:
            continue

    return False


def is_login_page(page) -> bool:
    text = visible_text(page)

    return (
        "dispMemberLoginForm" in page.url
        or ("로그인" in text and "비밀번호" in text and not is_logged_in(page))
    )


def is_write_page(page) -> bool:
    if "dispBoardWrite" in page.url:
        return True

    return first_visible(
        page,
        [
            'input[name="title"]',
            'input[name="document_title"]',
            'textarea[name="content"]',
            "iframe.cke_wysiwyg_frame",
            '[contenteditable="true"]',
        ],
        timeout=1000,
    ) is not None


def login_if_needed(page, board: str, user_id: str, password: str) -> None:
    print("opening FM Korea board before writing", flush=True)
    page.goto(board_url(board), wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(1500)

    if is_logged_in(page):
        print("FM Korea already logged in", flush=True)
        return

    print("opening FM Korea login before writing", flush=True)
    page.goto("https://www.fmkorea.com/index.php?act=dispMemberLoginForm", wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(1500)

    fill_first(
        page,
        [
            'input[name="user_id"]',
            'input[name="userId"]',
            'input[id*="user"]',
            'input[id*="id"]',
            'input[type="email"]',
            'input[type="text"]',
        ],
        user_id,
        "login id",
    )
    fill_first(
        page,
        [
            'input[name="password"]',
            'input[name="user_pw"]',
            'input[id*="password"]',
            'input[id*="pass"]',
            'input[type="password"]',
        ],
        password,
        "login password",
    )
    print("filled FM Korea login credentials", flush=True)

    login_button = first_visible(
        page,
        [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("로그인")',
            'a:has-text("로그인")',
        ],
        timeout=2500,
    )
    if login_button is not None:
        login_button.click()
    else:
        page.keyboard.press("Enter")

    try:
        page.wait_for_load_state("domcontentloaded", timeout=45000)
    except Exception:
        pass
    page.wait_for_timeout(3000)

    if not is_logged_in(page):
        screenshot_path = save_debug_snapshot(page, board, "login-failed")
        raise RuntimeError(f"FM Korea login did not complete. screenshot={screenshot_path}")


def open_write_page(page, board: str) -> None:
    page.goto(write_url(board), wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(2500)
    if is_write_page(page):
        return

    page.goto(board_url(board), wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(2000)
    write_link = first_visible(
        page,
        [
            'a[href*="dispBoardWrite"]',
            'a:has-text("글쓰기")',
            'button:has-text("글쓰기")',
            ".btn_write",
        ],
        timeout=5000,
    )
    if write_link is None:
        screenshot_path = save_debug_snapshot(page, board, "write-link-missing")
        raise RuntimeError(f"Could not find FM Korea write link. screenshot={screenshot_path}")

    href = write_link.get_attribute("href", timeout=1000)
    write_link.scroll_into_view_if_needed(timeout=3000)
    write_link.click(timeout=5000)
    page.wait_for_timeout(1500)
    if not is_write_page(page) and href:
        page.goto(href, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(2500)

    if not is_write_page(page):
        screenshot_path = save_debug_snapshot(page, board, "write-page-not-ready")
        raise RuntimeError(f"FM Korea write page is not ready. url={page.url}. screenshot={screenshot_path}")


def select_category_if_present(page) -> None:
    result = page.evaluate(
        """
        () => {
            const selects = Array.from(document.querySelectorAll('select'));
            const categorySelect = selects.find(select => {
                const text = select.innerText || '';
                return select.name === 'category'
                    || select.name === 'category_srl'
                    || select.id === 'category'
                    || text.includes('주식')
                    || text.includes('일반')
                    || text.includes('자유');
            });
            if (!categorySelect) return 'none';

            const options = Array.from(categorySelect.options || []);
            const preferred = options.find(option => option.textContent.includes('주식'))
                || options.find(option => option.textContent.includes('일반'))
                || options.find(option => option.textContent.includes('자유'))
                || options.find(option => option.value && !option.disabled);
            if (!preferred || !preferred.value) return 'empty';

            categorySelect.value = preferred.value;
            categorySelect.dispatchEvent(new Event('input', { bubbles: true }));
            categorySelect.dispatchEvent(new Event('change', { bubbles: true }));
            return `${preferred.textContent.trim()}:${preferred.value}`;
        }
        """
    )
    print(f"FM Korea category selection: {result}", flush=True)


def fill_body(page, body: str) -> None:
    editor_api_result = page.evaluate(
        """
        ({ body }) => {
            if (window.CKEDITOR && window.CKEDITOR.instances) {
                const instances = Object.values(window.CKEDITOR.instances);
                if (instances.length > 0) {
                    instances[0].setData(body.replace(/\\n/g, '<br>'));
                    instances[0].updateElement();
                    return 'ckeditor';
                }
            }
            if (window.tinyMCE && window.tinyMCE.activeEditor) {
                window.tinyMCE.activeEditor.setContent(body.replace(/\\n/g, '<br>'));
                return 'tinymce';
            }
            const textarea = document.querySelector(
                'textarea[name="content"], textarea[name="memo"], textarea[id*="content"], textarea'
            );
            if (textarea) {
                textarea.value = body;
                textarea.dispatchEvent(new Event('input', { bubbles: true }));
                textarea.dispatchEvent(new Event('change', { bubbles: true }));
                return 'textarea-dom';
            }
            return '';
        }
        """,
        {"body": body},
    )
    if editor_api_result:
        print(f"filled FM Korea post body via {editor_api_result}", flush=True)
        return

    editor = first_visible(
        page,
        [
            "iframe.cke_wysiwyg_frame",
            'iframe[id*="cke"]',
            '[contenteditable="true"]',
            ".cke_editable",
            "textarea",
        ],
        timeout=8000,
    )
    if editor is None:
        screenshot_path = save_debug_snapshot(page, "stock", "body-editor-missing")
        raise RuntimeError(f"Could not find FM Korea post body editor. screenshot={screenshot_path}")

    tag_name = editor.evaluate("el => el.tagName.toLowerCase()")
    if tag_name == "iframe":
        frame = editor.element_handle().content_frame()
        if frame is None:
            raise RuntimeError("Could not access FM Korea post body iframe.")
        target = first_visible(frame, ["body[contenteditable='true']", "body", "[contenteditable='true']"], timeout=1500)
        if target is None:
            screenshot_path = save_debug_snapshot(page, "stock", "body-iframe-missing")
            raise RuntimeError(f"Could not find FM Korea iframe editor body. screenshot={screenshot_path}")
        target.fill(body)
        print("filled FM Korea post body via iframe", flush=True)
        return

    editor.fill(body)
    print("filled FM Korea post body via contenteditable", flush=True)


def prepare_upload_image(page, image_path: str) -> Path:
    path = Path(image_path)
    if path.suffix.lower() != ".svg":
        return path

    png_path = Path(tempfile.gettempdir()) / f"{path.stem}-{int(time.time())}.png"
    svg = path.read_text(encoding="utf-8")
    render_page = page.context.new_page()
    try:
        render_page.set_content(
            f'<html><body style="margin:0;background:#fff">{svg}</body></html>',
            wait_until="load",
        )
        svg_locator = render_page.locator("svg").first
        svg_locator.wait_for(state="visible", timeout=5000)
        svg_locator.screenshot(path=str(png_path), omit_background=False)
    finally:
        render_page.close()

    print(f"converted image for upload: {path.name} -> {png_path.name}", flush=True)
    return png_path


def upload_image_if_present(page, image_path: str | None) -> None:
    if not image_path:
        return

    path = Path(image_path)
    if not path.exists():
        raise RuntimeError(f"Image file does not exist: {path}")

    upload_path = prepare_upload_image(page, str(path))
    upload_errors: list[str] = []
    file_inputs = page.locator('input[type="file"]')
    for index in range(file_inputs.count()):
        try:
            file_inputs.nth(index).set_input_files(str(upload_path), timeout=3000)
            page.wait_for_timeout(2500)
            print(f"attached image: {upload_path.name}", flush=True)
            return
        except Exception as exc:
            upload_errors.append(f"input[type=file][{index}]: {exc}")

    for selector in [
        'button:has-text("파일")',
        'button:has-text("사진")',
        'button:has-text("이미지")',
        'a:has-text("파일")',
        'a:has-text("사진")',
        'a:has-text("이미지")',
        ".cke_button__image",
    ]:
        try:
            with page.expect_file_chooser(timeout=3000) as file_chooser_info:
                page.locator(selector).first.click(timeout=3000)
            file_chooser_info.value.set_files(str(upload_path))
            page.wait_for_timeout(2500)
            print(f"attached image: {upload_path.name}", flush=True)
            return
        except Exception as exc:
            upload_errors.append(f"{selector}: {exc}")

    screenshot_path = save_debug_snapshot(page, "image", "upload-failed")
    raise RuntimeError(
        "FM Korea image upload failed. "
        f"screenshot={screenshot_path}. attempts={' | '.join(upload_errors[:8])}"
    )


def assert_post_submitted(page, board: str, title: str) -> str:
    page.wait_for_timeout(4000)
    current_url = page.url
    text = visible_text(page)
    if "dispBoardWrite" not in current_url and title.strip() and title.strip() in text:
        return current_url
    if "document_srl=" in current_url or current_url.rstrip("/").split("/")[-1].isdigit():
        return current_url

    screenshot_path = save_debug_snapshot(page, board, "not-submitted")
    raise RuntimeError(f"FM Korea submit was not confirmed. url={current_url}. screenshot={screenshot_path}")


def post_board(page, board: str, title: str, body: str, image_path: str | None) -> None:
    user_id = os.environ.get("FMKOREA_ID", "").strip()
    password = os.environ.get("FMKOREA_PASSWORD", "").strip()
    if not user_id or not password:
        raise RuntimeError("FMKOREA_ID and FMKOREA_PASSWORD are required.")

    login_if_needed(page, board, user_id, password)
    open_write_page(page, board)
    select_category_if_present(page)

    fill_first(
        page,
        [
            'input[name="title"]',
            'input[name="document_title"]',
            'input[id*="title"]',
            'input[type="text"]',
        ],
        title,
        "post title",
    )
    fill_body(page, body)
    upload_image_if_present(page, image_path)

    submit_button = first_visible(
        page,
        [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("등록")',
            'a:has-text("등록")',
            'button:has-text("작성")',
            'a:has-text("작성")',
        ],
        timeout=5000,
    )
    if submit_button is None:
        screenshot_path = save_debug_snapshot(page, board, "submit-missing")
        raise RuntimeError(f"Could not find FM Korea submit button. screenshot={screenshot_path}")

    submit_button.click()
    post_url = assert_post_submitted(page, board, title)
    print(f"submitted FM Korea board: {board}", flush=True)
    print(f"POST_URL {post_url}", flush=True)


def run_live(title: str, body: str, board: str, image_path: str | None) -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is required. Run: python -m pip install playwright && "
            "python -m playwright install chromium"
        ) from exc

    headless = env_bool("FMKOREA_HEADLESS", False)
    slow_mo = int(os.environ.get("FMKOREA_SLOW_MO_MS", "100"))
    browser_channel = os.environ.get("FMKOREA_BROWSER_CHANNEL", "chrome").strip() or None
    user_data_dir = os.environ.get("FMKOREA_BROWSER_PROFILE", "").strip()

    with sync_playwright() as playwright:
        launch_options = {"headless": headless, "slow_mo": slow_mo}
        if browser_channel:
            launch_options["channel"] = browser_channel

        if user_data_dir:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir,
                locale="ko-KR",
                **launch_options,
            )
            page = context.pages[0] if context.pages else context.new_page()
            try:
                post_board(page, board, title, body, image_path)
            finally:
                context.close()
        else:
            browser = playwright.chromium.launch(**launch_options)
            context = browser.new_context(locale="ko-KR")
            page = context.new_page()
            try:
                post_board(page, board, title, body, image_path)
            finally:
                browser.close()

    return 0


def main() -> int:
    args = parse_args()
    print(f"script version: {SCRIPT_VERSION}", flush=True)
    print(f"script path: {Path(__file__).resolve()}", flush=True)
    title = read_text(args.title_file)
    body = read_text(args.body_file)
    board = args.board.strip() or "stock"

    if args.dry_run:
        print("dry-run: FM Korea post script loaded")
        print(f"dry-run: board={board}")
        print(f"dry-run: titleLength={len(title)} bodyLength={len(body)}")
        print(f"dry-run: image={'yes' if args.image else 'no'}")
        return 0

    return run_live(title, body, board, args.image)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERR {exc}", file=sys.stderr)
        raise SystemExit(1)
