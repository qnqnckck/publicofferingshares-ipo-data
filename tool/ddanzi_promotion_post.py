#!/usr/bin/env python3
"""Post IPO promotion content to Ddanzi boards."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

SCRIPT_VERSION = "ddanzi-login-v3"


def read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Post promotion content to Ddanzi.")
    parser.add_argument("--title-file", required=True)
    parser.add_argument("--body-file", required=True)
    parser.add_argument("--board", default="stockclub")
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


def click_first(page, selectors: list[str], label: str) -> None:
    locator = first_visible(page, selectors)
    if locator is None:
        raise RuntimeError(f"Could not find {label} button.")
    locator.click()


def fill_first(page, selectors: list[str], value: str, label: str) -> None:
    errors: list[str] = []
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=1500)
            input_type = (locator.get_attribute("type", timeout=500) or "").lower()
            if input_type in {"checkbox", "radio", "hidden", "submit", "button", "file"}:
                errors.append(f"{selector}: skipped input type {input_type}")
                continue
            locator.fill(value)
            return
        except Exception as exc:
            errors.append(f"{selector}: {exc}")

    screenshot_path = page_snapshot_path("field", f"{label.replace(' ', '-')}-missing")
    page.screenshot(path=str(screenshot_path), full_page=True)
    raise RuntimeError(
        f"Could not find {label} field. screenshot={screenshot_path}. "
        f"attempts={' | '.join(errors[:6])}"
    )


def page_snapshot_path(board: str, suffix: str) -> Path:
    snapshot_dir = Path(os.environ.get("IPO_PROMOTION_DEBUG_DIR", "tmp/promotion-debug"))
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    safe_board = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in board)

    return snapshot_dir / f"{int(time.time())}-ddanzi-{safe_board}-{suffix}.png"


def save_debug_snapshot(page, board: str, suffix: str) -> Path:
    screenshot_path = page_snapshot_path(board, suffix)
    page.screenshot(path=str(screenshot_path), full_page=True)
    html_path = screenshot_path.with_suffix(".html")
    html_path.write_text(page.content(), encoding="utf-8")
    return screenshot_path


def visible_text(page) -> str:
    try:
        return page.locator("body").inner_text(timeout=2000)
    except Exception:
        return ""


def has_visible_password_field(page) -> bool:
    try:
        return page.locator('input[type="password"]').first.is_visible(timeout=1000)
    except Exception:
        return False


def is_logged_in(page) -> bool:
    for selector in [
        'a[href*="procMemberLogout"]',
        'a[href*="dispMemberLogout"]',
        'button:has-text("로그아웃")',
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
        or ("아이디와 비밀번호" in text and has_visible_password_field(page))
        or ("비밀번호" in text and "로그인" in text and has_visible_password_field(page))
    )


def is_write_page(page) -> bool:
    if "dispBoardWrite" in page.url and not is_login_page(page):
        return True

    try:
        title_field = page.locator(
            'input[type="text"][name="title"], input[type="text"][name="subject"]'
        ).first
        return title_field.is_visible(timeout=1000) and not has_visible_password_field(page)
    except Exception:
        return False


def submit_login_form(page, return_url: str) -> None:
    page.evaluate(
        """
        ({ returnUrl }) => {
            const form = document.querySelector('#fo_member_login') || document.querySelector('form[action*="procMemberLogin"]');
            if (!form) return false;

            for (const name of ['success_return_url', 'error_return_url']) {
                let input = form.querySelector(`input[name="${name}"]`);
                if (!input) {
                    input = document.createElement('input');
                    input.type = 'hidden';
                    input.name = name;
                    form.appendChild(input);
                }
                input.value = returnUrl;
            }

            return true;
        }
        """,
        {"returnUrl": return_url},
    )

    try:
        page.locator('input[name="password"], input[type="password"]').first.press("Enter", timeout=1500)
        page.wait_for_timeout(2500)
    except Exception:
        pass

    if not is_login_page(page):
        return

    try:
        page.evaluate(
            """
            () => {
                const form = document.querySelector('#fo_member_login') || document.querySelector('form[action*="procMemberLogin"]');
                if (!form) return false;
                if (form.requestSubmit) form.requestSubmit();
                else form.submit();
                return true;
            }
            """
        )
        try:
            page.wait_for_load_state("domcontentloaded", timeout=45000)
        except Exception:
            pass
        page.wait_for_timeout(2500)
    except Exception:
        pass


def login_via_http(page, user_id: str, password: str, return_url: str) -> str:
    print("trying Ddanzi login via HTTP POST", flush=True)
    form_payload = page.evaluate(
        """
        ({ userId, password, returnUrl }) => {
            const form = document.querySelector('#fo_member_login') || document.querySelector('form[action*="procMemberLogin"]');
            if (!form) return null;

            const payload = {};
            for (const input of form.querySelectorAll('input[name]')) {
                if ((input.type === 'checkbox' || input.type === 'radio') && !input.checked) continue;
                payload[input.name] = input.value || '';
            }
            payload.user_id = userId;
            payload.password = password;
            payload.success_return_url = returnUrl;
            payload.error_return_url = returnUrl;
            payload.act = 'procMemberLogin';
            return {
                action: form.action || 'https://www.ddanzi.com/index.php?act=procMemberLogin',
                payload,
            };
        }
        """,
        {"userId": user_id, "password": password, "returnUrl": return_url},
    )
    if not form_payload:
        print("Ddanzi login POST skipped: login form not found", flush=True)
        return "skipped"

    response = page.context.request.post(
        form_payload["action"],
        form=form_payload["payload"],
        timeout=45000,
    )
    print(f"Ddanzi login POST status: {response.status}", flush=True)
    page.goto(return_url, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(2500)
    if is_write_page(page):
        return "write"
    if is_logged_in(page):
        return "logged-in"

    return "failed"


def board_url(board: str) -> str:
    return f"https://www.ddanzi.com/{quote(board)}"


def write_url(board: str) -> str:
    return f"https://www.ddanzi.com/index.php?mid={quote(board)}&act=dispBoardWrite"


def open_write_page_from_board(page, board: str) -> bool:
    print("opening Ddanzi board list before writing", flush=True)
    page.goto(board_url(board), wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(2500)

    if is_write_page(page):
        return True

    selectors = [
        'a.writeButton[href*="dispBoardWrite"]',
        'a[href*="dispBoardWrite"]',
        'a:has-text("글쓰기")',
        'button:has-text("글쓰기")',
        'a:has-text("쓰기")',
        'button:has-text("쓰기")',
        '.btn_write',
        '.write',
    ]

    clicked = False
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() == 0:
                continue
            href = locator.get_attribute("href", timeout=1000)
            locator.scroll_into_view_if_needed(timeout=3000)
            locator.click(timeout=5000)
            clicked = True
            print(f"clicked Ddanzi write link: {selector}", flush=True)
            if href:
                page.wait_for_timeout(700)
                if not is_write_page(page):
                    print(f"opening Ddanzi write href directly: {href}", flush=True)
                    page.goto(href, wait_until="domcontentloaded", timeout=45000)
            break
        except Exception:
            continue

    if not clicked:
        return False

    try:
        page.wait_for_load_state("domcontentloaded", timeout=45000)
    except Exception:
        pass
    page.wait_for_timeout(2500)
    if is_write_page(page):
        return True

    categories = page.evaluate(
        """
        () => Array.from(document.querySelectorAll('a[href*="category="]'))
            .map(anchor => new URL(anchor.href, location.href).searchParams.get('category'))
            .filter(Boolean)
            .filter((value, index, values) => values.indexOf(value) === index)
            .slice(0, 8)
        """
    )
    for category in categories:
        category_write_url = (
            f"https://www.ddanzi.com/index.php?mid={quote(board)}"
            f"&category={quote(str(category))}&act=dispBoardWrite"
        )
        print(f"opening Ddanzi category write URL: {category_write_url}", flush=True)
        page.goto(category_write_url, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(2000)
        if is_write_page(page):
            return True

    return is_write_page(page)


def login_if_needed(page, user_id: str, password: str, return_url: str) -> None:
    print("opening Ddanzi login before writing")
    login_url = "https://www.ddanzi.com/index.php?act=dispMemberLoginForm&mid=member"
    page.goto(login_url, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(1500)

    if is_logged_in(page):
        page.goto(return_url, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(1500)
        return

    fill_first(
        page,
        [
            'input[name="user_id"]',
            'input[name="userId"]',
            'input[name="email_address"]',
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
            'input[name="password1"]',
            'input[id*="pass"]',
            'input[id*="pw"]',
            'input[type="password"]',
        ],
        password,
        "login password",
    )
    print("filled Ddanzi login credentials", flush=True)

    try:
        http_login_result = login_via_http(page, user_id, password, return_url)
        if http_login_result == "write":
            return
        if http_login_result == "logged-in":
            print(f"Ddanzi login succeeded; direct write URL did not open: {page.url}", flush=True)
            return
    except Exception as exc:
        if "Ddanzi login succeeded" in str(exc):
            raise
        print(f"Ddanzi login POST fallback failed: {exc}", flush=True)

    login_button = first_visible(
        page,
        [
            'form[action*="procMemberLogin"] button[type="submit"]',
            'form button:has-text("로그인")',
            'button.btn:has-text("로그인")',
            'input[type="submit"][value*="로그인"]',
        ],
    )
    if login_button is None:
        screenshot_path = page_snapshot_path("login", "button-missing")
        page.screenshot(path=str(screenshot_path), full_page=True)
        raise RuntimeError(f"Could not find Ddanzi login button. screenshot={screenshot_path}")
    try:
        login_button.click(timeout=2000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=45000)
        except Exception:
            pass
        page.wait_for_timeout(2500)
    except Exception:
        pass

    if is_login_page(page):
        submit_login_form(page, return_url)

    if is_login_page(page):
        screenshot_path = save_debug_snapshot(page, "login", "failed")
        raise RuntimeError(f"Ddanzi login did not complete. screenshot={screenshot_path}")

    page.goto(return_url, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(3000)

    if is_login_page(page) or not is_write_page(page):
        screenshot_path = save_debug_snapshot(page, "stockclub", "write-page-not-opened")
        raise RuntimeError(
            f"Ddanzi write page did not open after login. url={page.url}. "
            f"screenshot={screenshot_path}"
        )


def fill_body(page, body: str) -> None:
    editor_api_result = page.evaluate(
        """
        ({ body }) => {
            if (window.CKEDITOR && window.CKEDITOR.instances) {
                const instances = Object.values(window.CKEDITOR.instances);
                if (instances.length > 0) {
                    instances[0].setData(body);
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
        print(f"filled Ddanzi post body via {editor_api_result}")
        return

    textarea = first_visible(
        page,
        [
            'textarea[name="content"]',
            'textarea[name="memo"]',
            'textarea[id*="content"]',
            "textarea",
        ],
        timeout=8000,
    )
    if textarea is not None:
        textarea.fill(body)
        print("filled Ddanzi post body via textarea")
        return

    editor = first_visible(
        page,
        [
            "iframe.cke_wysiwyg_frame",
            'iframe[title*="Rich Text"]',
            'iframe[title*="리치"]',
            'iframe[id*="cke"]',
            '[contenteditable="true"]',
            ".cke_editable",
            ".note-editable",
            "iframe",
        ],
        timeout=8000,
    )
    if editor is None:
        screenshot_path = page_snapshot_path("stockclub", "body-editor-missing")
        page.screenshot(path=str(screenshot_path), full_page=True)
        raise RuntimeError(f"Could not find Ddanzi post body editor. screenshot={screenshot_path}")

    tag_name = editor.evaluate("el => el.tagName.toLowerCase()")
    if tag_name == "iframe":
        frame = editor.element_handle().content_frame()
        if frame is None:
            raise RuntimeError("Could not access Ddanzi post body iframe.")
        target = first_visible(
            frame,
            ["body[contenteditable='true']", "body", "[contenteditable='true']"],
            timeout=1500,
        )
        if target is None:
            screenshot_path = page_snapshot_path("stockclub", "body-iframe-missing")
            page.screenshot(path=str(screenshot_path), full_page=True)
            raise RuntimeError(f"Could not find Ddanzi iframe editor body. screenshot={screenshot_path}")
        target.fill(body)
        print("filled Ddanzi post body via iframe")
        return

    editor.fill(body)
    print("filled Ddanzi post body via contenteditable")


def select_category_if_present(page) -> None:
    result = page.evaluate(
        """
        () => {
            const selects = Array.from(document.querySelectorAll('select'));
            const categorySelect = selects.find(select => {
                const text = select.innerText || '';
                return select.name === 'category'
                    || select.id === 'category'
                    || select.id === 'board_category'
                    || text.includes('투자일반')
                    || text.includes('자유');
            });
            if (!categorySelect) return 'none';

            const options = Array.from(categorySelect.options || []);
            const preferred = options.find(option => option.textContent.includes('투자일반'))
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
    print(f"Ddanzi category selection: {result}", flush=True)


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

    print(f"converted image for upload: {path.name} -> {png_path.name}")
    return png_path


def upload_image_if_present(page, image_path: str | None) -> None:
    if not image_path:
        return

    path = Path(image_path)
    if not path.exists():
        raise RuntimeError(f"Image file does not exist: {path}")

    upload_path = prepare_upload_image(page, str(path))
    file_inputs = page.locator('input[type="file"]')
    input_count = file_inputs.count()
    upload_errors: list[str] = []
    for index in range(input_count):
        try:
            file_inputs.nth(index).set_input_files(str(upload_path), timeout=3000)
            page.wait_for_timeout(2000)
            print(f"attached image: {upload_path.name}")
            return
        except Exception as exc:
            upload_errors.append(f"input[type=file][{index}]: {exc}")

    for selector in [
        'button:has-text("파일")',
        'button:has-text("이미지")',
        'a:has-text("파일")',
        'a:has-text("이미지")',
        ".cke_button__image",
    ]:
        try:
            with page.expect_file_chooser(timeout=3000) as file_chooser_info:
                page.locator(selector).first.click(timeout=3000)
            file_chooser_info.value.set_files(str(upload_path))
            page.wait_for_timeout(2000)
            print(f"attached image: {upload_path.name}")
            return
        except Exception as exc:
            upload_errors.append(f"{selector}: {exc}")

    screenshot_path = page_snapshot_path("image", "upload-failed")
    page.screenshot(path=str(screenshot_path), full_page=True)
    raise RuntimeError(
        "Ddanzi image upload failed. "
        f"screenshot={screenshot_path}. "
        f"attempts={' | '.join(upload_errors[:8])}"
    )


def assert_post_submitted(page, board: str, title: str) -> str:
    page.wait_for_timeout(3000)
    current_url = page.url
    text = visible_text(page)
    if title.strip() and title.strip() in text and "dispBoardWrite" not in current_url:
        return current_url
    if "document_srl=" in current_url:
        return current_url

    screenshot_path = save_debug_snapshot(page, board, "not-submitted")
    raise RuntimeError(
        f"Ddanzi submit was not confirmed. url={current_url}. "
        f"screenshot={screenshot_path}"
    )


def post_board(page, board: str, title: str, body: str, image_path: str | None) -> None:
    user_id = os.environ.get("DDANZI_ID", "").strip()
    password = os.environ.get("DDANZI_PASSWORD", "").strip()
    if not user_id or not password:
        raise RuntimeError("DDANZI_ID and DDANZI_PASSWORD are required.")

    target_url = write_url(board)
    print(f"opening Ddanzi board: {board}")
    login_if_needed(page, user_id, password, target_url)

    if "dispBoardWrite" not in page.url:
        page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(1500)

    if not is_write_page(page) and is_logged_in(page):
        open_write_page_from_board(page, board)

    if is_login_page(page) or not is_write_page(page):
        screenshot_path = save_debug_snapshot(page, board, "write-page-not-opened")
        raise RuntimeError(
            f"Ddanzi write page is not ready. url={page.url}. "
            f"screenshot={screenshot_path}"
        )

    select_category_if_present(page)
    fill_first(
        page,
        [
            'input[type="text"][name="title"]',
            'input[type="text"][name="subject"]',
            'input[type="text"][id*="title"]',
            'input:not([type]), input[type="text"]',
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
    post_url = assert_post_submitted(page, board, title)
    print(f"submitted Ddanzi board: {board}")
    print(f"POST_URL {post_url}")


def run_live(title: str, body: str, board: str, image_path: str | None) -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is required. Run: python -m pip install playwright && "
            "python -m playwright install chromium"
        ) from exc

    headless = env_bool("DDANZI_HEADLESS", False)
    slow_mo = int(os.environ.get("DDANZI_SLOW_MO_MS", "100"))

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless, slow_mo=slow_mo)
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
    board = args.board.strip() or "stockclub"

    if args.dry_run:
        print("dry-run: Ddanzi post script loaded")
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
