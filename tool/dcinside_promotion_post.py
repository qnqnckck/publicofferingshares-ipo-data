#!/usr/bin/env python3
"""Post IPO promotion content to DCInside galleries.

The admin server passes title/body via temporary files so Korean text and long
posts do not have to travel through command-line arguments.
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse


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


def fill_first_empty(page, selectors: list[str], value: str) -> bool:
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=800)
            current = locator.input_value(timeout=500).strip()
            if not current:
                locator.fill(value)
            return True
        except Exception:
            continue

    return False


def click_first(page, selectors: list[str], label: str) -> None:
    locator = first_visible(page, selectors)
    if locator is None:
        raise RuntimeError(f"Could not find {label} button.")
    locator.click()


def click_dcinside_submit(page) -> None:
    exact_submit = page.get_by_text("등록", exact=True)
    matches = exact_submit.all()
    for locator in reversed(matches):
        try:
            if not locator.is_visible(timeout=500):
                continue
            text = locator.inner_text(timeout=500).strip()
            if text == "등록":
                locator.click()
                return
        except Exception:
            continue

    for selector in [
        '.btn_blue:has-text("등록")',
        '.btn_lightblue:has-text("등록")',
        'button:has-text("등록")',
        'a:has-text("등록")',
        'input[value="등록"]',
    ]:
        locator = page.locator(selector).last
        try:
            locator.wait_for(state="visible", timeout=1000)
            label = ""
            try:
                label = locator.inner_text(timeout=500).strip()
            except Exception:
                label = locator.get_attribute("value", timeout=500) or ""
            if "등록(" in label:
                continue
            locator.click()
            return
        except Exception:
            continue

    raise RuntimeError("Could not find DCInside submit button.")


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
    force_login = env_bool("DCINSIDE_FORCE_LOGIN", True)

    if not force_login:
        page.goto(return_url, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(1500)
        if "sign.dcinside.com" not in page.url and "login" not in page.url.lower():
            return

    if force_login:
        print("opening DCInside login before writing")

    login_url = "https://sign.dcinside.com/login?s_url=" + quote(return_url, safe="")
    page.goto(login_url, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(1500)

    login_id_field = first_visible(
        page,
        [
            'input[name="user_id"]',
            'input[name="id"]',
            'input[id*="user"]',
            'input[id*="id"]',
            'input[type="text"]',
        ],
        timeout=2500,
    )
    login_password_field = first_visible(
        page,
        [
            'input[name="pw"]',
            'input[name="password"]',
            'input[id*="pw"]',
            'input[id*="pass"]',
            'input[type="password"]',
        ],
        timeout=2500,
    )

    if login_id_field is None and login_password_field is None:
        page.goto(return_url, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(1500)
        return

    if login_id_field is None or login_password_field is None:
        screenshot_path = page_snapshot_path("login", "form-missing")
        page.screenshot(path=str(screenshot_path), full_page=True)
        raise RuntimeError(f"Could not find DCInside login form. screenshot={screenshot_path}")

    login_id_field.fill(user_id)
    login_password_field.fill(password)
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

    page.goto(return_url, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(1500)


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


def editor_image_count(page) -> int:
    try:
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
            return 0

        tag_name = editor.evaluate("el => el.tagName.toLowerCase()")
        if tag_name == "iframe":
            frame = editor.element_handle().content_frame()
            if frame is None:
                return 0
            return frame.locator("img").count()

        return editor.locator("img").count()
    except Exception:
        return 0


def focus_editor(page) -> bool:
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
        return False

    try:
        tag_name = editor.evaluate("el => el.tagName.toLowerCase()")
        if tag_name == "iframe":
            frame = editor.element_handle().content_frame()
            if frame is None:
                return False
            target = first_visible(
                frame,
                ["body[contenteditable='true']", "body", "[contenteditable='true']"],
                timeout=1500,
            )
            if target is None:
                return False
            target.click()
            return True

        editor.click()
        return True
    except Exception:
        return False


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


def try_paste_image(page, upload_path: Path, upload_errors: list[str]) -> bool:
    try:
        page.context.grant_permissions(
            ["clipboard-read", "clipboard-write"],
            origin="https://gall.dcinside.com",
        )
    except Exception:
        pass

    mime = "image/jpeg" if upload_path.suffix.lower() in {".jpg", ".jpeg"} else "image/png"
    encoded = base64.b64encode(upload_path.read_bytes()).decode("ascii")
    clipboard_script = """
        async ({ mime, encoded }) => {
            if (!navigator.clipboard || !window.ClipboardItem) return false;
            const bytes = Uint8Array.from(atob(encoded), char => char.charCodeAt(0));
            const blob = new Blob([bytes], { type: mime });
            await navigator.clipboard.write([new ClipboardItem({ [mime]: blob })]);
            return true;
        }
    """

    try:
        copied = bool(page.evaluate(clipboard_script, {"mime": mime, "encoded": encoded}))
        if not copied:
            upload_errors.append("clipboard paste: browser clipboard image write is unavailable")
            return False
        if not focus_editor(page):
            upload_errors.append("clipboard paste: editor focus failed")
            return False
        page.keyboard.press("Control+V")
        if wait_for_editor_image(page, upload_path, "clipboard paste"):
            print(f"attached image by clipboard paste: {upload_path.name}")
            return True
        upload_errors.append("clipboard paste: pasted image was not inserted into editor")
    except Exception as exc:
        upload_errors.append(f"clipboard paste: {exc}")

    return False


def wait_for_editor_image(page, image_path: Path, label: str) -> bool:
    for _ in range(15):
        if editor_image_count(page) > 0:
            print(f"image visible in editor after {label}: {image_path.name}")
            return True
        page.wait_for_timeout(1000)

    return False


def try_upload_via_visible_click(page, selector: str, upload_path: Path, upload_errors: list[str]) -> bool:
    locator = page.locator(selector)
    try:
        count = locator.count()
    except Exception as exc:
        upload_errors.append(f"{selector}: {exc}")
        return False

    for index in range(count):
        candidate = locator.nth(index)
        try:
            if not candidate.is_visible(timeout=500):
                continue
            candidate.scroll_into_view_if_needed(timeout=1000)
            with page.expect_file_chooser(timeout=3000) as file_chooser_info:
                candidate.click(timeout=3000)
            file_chooser_info.value.set_files(str(upload_path))
            if wait_for_editor_image(page, upload_path, selector):
                print(f"attached image: {upload_path.name}")
                return True
            upload_errors.append(f"{selector}: file selected but image was not inserted into editor")
        except Exception as exc:
            upload_errors.append(f"{selector}: {exc}")

    return False


def upload_image_if_present(page, image_path: str | None) -> None:
    if not image_path:
        return

    path = Path(image_path)
    if not path.exists():
        raise RuntimeError(f"Image file does not exist: {path}")

    upload_path = prepare_upload_image(page, str(path))
    upload_errors: list[str] = []

    if try_paste_image(page, upload_path, upload_errors):
        return

    for selector in [
        '.write_option :text-is("이미지")',
        '.write_type :text-is("이미지")',
        '.editor_wrap :text-is("이미지")',
        '.tx-toolbar :text-is("이미지")',
        '.tx_toolbar :text-is("이미지")',
        'button:has-text("이미지")',
        'a:has-text("이미지")',
        'label:has-text("이미지")',
        '.btn_img',
        '.btn_image',
    ]:
        if try_upload_via_visible_click(page, selector, upload_path, upload_errors):
            return

    file_inputs = page.locator('input[type="file"]')
    input_count = file_inputs.count()
    for index in range(input_count):
        file_input = file_inputs.nth(index)
        try:
            file_input.set_input_files(str(upload_path), timeout=3000)
            if wait_for_editor_image(page, upload_path, f"input[type=file][{index}]"):
                print(f"attached image: {upload_path.name}")
                return
            upload_errors.append(f"input[type=file][{index}]: file selected but image was not inserted into editor")
        except Exception as exc:
            upload_errors.append(f"input[type=file][{index}]: {exc}")

    screenshot_path = page_snapshot_path("image", "upload-failed")
    page.screenshot(path=str(screenshot_path), full_page=True)
    raise RuntimeError(
        "DCInside image upload failed. "
        f"screenshot={screenshot_path}. "
        f"attempts={' | '.join(upload_errors[:8])}"
    )


def fill_guest_identity(page, user_id: str, password: str) -> None:
    nickname = os.environ.get("DCINSIDE_NICKNAME", "").strip() or "공갤러"
    fill_first_empty(
        page,
        [
            'input[name="name"]',
            'input[name="nick_name"]',
            'input[name="nickname"]',
            'input[id*="name"]',
            'input[id*="nick"]',
            'input[placeholder*="닉"]',
        ],
        nickname,
    )
    filled_password = fill_first_empty(
        page,
        [
            'input[name="password"]',
            'input[name="pw"]',
            'input[name="user_pw"]',
            'input[id*="password"]',
            'input[id*="pw"]',
            'input[type="password"]',
        ],
        password,
    )
    if filled_password:
        print("filled DCInside guest post password")


def dcinside_write_url(gallery: str) -> str:
    regular_galleries = {"gongmozoo"}
    board_path = "board" if gallery in regular_galleries else "mgallery/board"

    return f"https://gall.dcinside.com/{board_path}/write/?id={quote(gallery)}"


def assert_post_submitted(
    page,
    gallery: str,
    write_url: str,
    title: str,
    dialog_messages: list[str],
) -> str:
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
    normalized_title = title.strip()

    parsed_url = urlparse(current_url)
    query = parse_qs(parsed_url.query)
    is_current_gallery = query.get("id", [""])[0] == gallery
    is_list_page = parsed_url.path.endswith("/lists/")
    is_view_page = parsed_url.path.endswith("/view/") and bool(query.get("no"))

    if is_current_gallery and is_list_page and normalized_title and normalized_title in body_text:
        return current_url
    if is_current_gallery and is_view_page:
        return current_url

    blocking_keywords = [
        "자동등록방지",
        "보안코드",
        "캡차",
        "captcha",
        "권한",
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

    screenshot_path = page_snapshot_path(gallery, "not-submitted")
    page.screenshot(path=str(screenshot_path), full_page=True)
    detail = f" keyword={found_keyword}" if found_keyword else ""
    raise RuntimeError(
        f"DCInside submit was not confirmed. url={current_url}{detail}. "
        f"screenshot={screenshot_path}"
    )


def submitted_post_image_count(page) -> int:
    selectors = [
        ".writing_view_box img",
        ".write_div img",
        ".view_content img",
        ".writing_view img",
        ".view_content_wrap img",
    ]
    for selector in selectors:
        try:
            count = page.locator(selector).count()
            if count > 0:
                return count
        except Exception:
            continue

    return 0


def assert_submitted_post_has_image(page, gallery: str, post_url: str, title: str) -> str:
    if "/lists/" in post_url:
        try:
            page.get_by_text(title.strip(), exact=True).first.click(timeout=5000)
            page.wait_for_load_state("domcontentloaded", timeout=45000)
            page.wait_for_timeout(1500)
            post_url = page.url
        except Exception:
            pass

    image_count = submitted_post_image_count(page)
    if image_count > 0:
        print(f"submitted post image count: {image_count}")
        return post_url

    screenshot_path = page_snapshot_path(gallery, "submitted-image-missing")
    page.screenshot(path=str(screenshot_path), full_page=True)
    raise RuntimeError(
        f"DCInside submitted post does not show an image. url={post_url}. "
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

    fill_guest_identity(page, user_id, password)

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

    click_dcinside_submit(page)
    post_url = assert_post_submitted(page, gallery, write_url, title, dialog_messages)
    if image_path:
        post_url = assert_submitted_post_has_image(page, gallery, post_url, title)
    print(f"submitted DCInside gallery: {gallery}")
    print(f"POST_URL {post_url}")


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
