from __future__ import annotations

import asyncio
import importlib
import logging
from typing import Any

from playwright.async_api import async_playwright


logger = logging.getLogger(__name__)


def _normalize_domain(domain: str) -> str:
    value = str(domain or "").strip().lower()
    value = value.replace("https://", "").replace("http://", "")
    value = value.split("/")[0]
    if value.startswith("www."):
        value = value[4:]
    return value


def _candidate_urls(domain: str) -> list[str]:
    d = _normalize_domain(domain)
    if not d:
        return []
    base = f"https://{d}"
    return [base, f"{base}/careers", f"{base}/jobs", f"{base}/about", f"{base}/blog"]


async def _crawl_with_crawl4ai(urls: list[str], timeout_seconds: int) -> dict[str, str]:
    crawl4ai = importlib.import_module("crawl4ai")
    markdown_by_url: dict[str, str] = {}

    async_web_crawler = getattr(crawl4ai, "AsyncWebCrawler", None)
    if async_web_crawler is None:
        raise RuntimeError("crawl4ai AsyncWebCrawler not found")

    async with async_web_crawler() as crawler:
        for url in urls:
            try:
                result = await asyncio.wait_for(crawler.arun(url=url), timeout=timeout_seconds)
            except Exception:
                continue
            markdown = str(getattr(result, "markdown", "") or "").strip()
            if markdown:
                markdown_by_url[url] = markdown[:12000]

    return markdown_by_url


async def _crawl_with_playwright(urls: list[str], timeout_seconds: int) -> dict[str, str]:
    pages: dict[str, str] = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        try:
            for url in urls:
                try:
                    await asyncio.wait_for(page.goto(url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000), timeout=timeout_seconds + 5)
                except Exception:
                    continue
                try:
                    text = await page.locator("body").inner_text(timeout=3000)
                except Exception:
                    text = ""
                text = " ".join(str(text).split()).strip()
                if text:
                    pages[url] = text[:12000]
        finally:
            await page.close()
            await context.close()
            await browser.close()
    return pages


async def crawl_company_domain(domain: str, timeout_seconds: int = 25) -> dict[str, Any]:
    urls = _candidate_urls(domain)
    if not urls:
        return {"domain": "", "pages": {}, "engine": "none"}

    try:
        pages = await _crawl_with_crawl4ai(urls, timeout_seconds)
        if pages:
            return {"domain": _normalize_domain(domain), "pages": pages, "engine": "crawl4ai"}
    except Exception as exc:  # noqa: BLE001
        logger.info("crawl4ai failed for %s: %s", domain, exc)

    pages = await _crawl_with_playwright(urls, timeout_seconds)
    return {"domain": _normalize_domain(domain), "pages": pages, "engine": "playwright"}
