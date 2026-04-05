from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Any
from urllib.parse import quote_plus

from playwright.async_api import BrowserContext
from playwright.async_api import Page
from playwright.async_api import async_playwright


def _clean_text(value: str | None) -> str:
    return " ".join((value or "").split()).strip()


def _role_matches_query(role: str, query: str) -> bool:
    role_l = role.lower()
    query_tokens = [token for token in query.lower().split() if token]
    if not query_tokens:
        return True
    return any(token in role_l for token in query_tokens)


async def _safe_text(node: Any, selector: str) -> str:
    try:
        text = await node.locator(selector).first.inner_text(timeout=1200)
    except Exception:
        return ""
    return _clean_text(text)


async def _parse_linkedin_jobs_page(page: Page, role: str) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []

    try:
        cards = page.locator("li:has(div.base-search-card)")
        card_count = await cards.count()
    except Exception:
        return jobs

    for idx in range(card_count):
        card = cards.nth(idx)
        company = await _safe_text(card, ".base-search-card__subtitle")
        title = await _safe_text(card, ".base-search-card__title")
        location = await _safe_text(card, ".job-search-card__location")
        date_posted = await _safe_text(card, "time")

        if not company or not title:
            continue
        if not _role_matches_query(title, role):
            continue

        jobs.append(
            {
                "company": company,
                "role": title,
                "location": location,
                "date_posted": date_posted,
                "source": "linkedin",
            }
        )

    return jobs


async def _parse_indeed_jobs_page(page: Page, role: str) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []

    selectors = ["#mosaic-provider-jobcards li", "div.job_seen_beacon"]
    cards = None
    for selector in selectors:
        try:
            loc = page.locator(selector)
            if await loc.count() > 0:
                cards = loc
                break
        except Exception:
            continue

    if cards is None:
        return jobs

    try:
        card_count = await cards.count()
    except Exception:
        return jobs

    for idx in range(card_count):
        card = cards.nth(idx)
        company = await _safe_text(card, "[data-testid='company-name']")
        title = await _safe_text(card, "h2.jobTitle")
        location = await _safe_text(card, "[data-testid='text-location']")
        date_posted = await _safe_text(card, "[data-testid='myJobsStateDate']")

        if not title:
            title = await _safe_text(card, "a span")
        if not company or not title:
            continue
        if not _role_matches_query(title, role):
            continue

        jobs.append(
            {
                "company": company,
                "role": title,
                "location": location,
                "date_posted": date_posted,
                "source": "indeed",
            }
        )

    return jobs


async def _scroll_page(page: Page) -> None:
    for _ in range(3):
        try:
            await page.mouse.wheel(0, 2500)
            await asyncio.sleep(0.8)
        except Exception:
            return


async def _scrape_linkedin_jobs(context: BrowserContext, role: str, max_pages: int) -> list[dict[str, str]]:
    collected: list[dict[str, str]] = []
    page = await context.new_page()
    query = quote_plus(role)

    try:
        for page_idx in range(max_pages):
            start = page_idx * 25
            url = f"https://www.linkedin.com/jobs/search/?keywords={query}&start={start}"
            await page.goto(url, wait_until="domcontentloaded", timeout=35000)
            await _scroll_page(page)
            jobs = await _parse_linkedin_jobs_page(page, role)
            if not jobs:
                break
            collected.extend(jobs)
    except Exception:
        return []
    finally:
        await page.close()

    return collected


async def _scrape_indeed_jobs(context: BrowserContext, role: str, max_pages: int) -> list[dict[str, str]]:
    collected: list[dict[str, str]] = []
    page = await context.new_page()
    query = quote_plus(role)

    try:
        for page_idx in range(max_pages):
            start = page_idx * 10
            url = f"https://www.indeed.com/jobs?q={query}&start={start}"
            await page.goto(url, wait_until="domcontentloaded", timeout=35000)
            await _scroll_page(page)
            jobs = await _parse_indeed_jobs_page(page, role)
            if not jobs:
                break
            collected.extend(jobs)
    except Exception:
        return []
    finally:
        await page.close()

    return collected


def _dedupe_jobs(jobs: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    seen_jobs: set[tuple[str, str, str, str]] = set()
    deduped: list[dict[str, str]] = []

    for item in jobs:
        company = _clean_text(item.get("company"))
        role = _clean_text(item.get("role"))
        location = _clean_text(item.get("location"))
        source = _clean_text(item.get("source"))
        date_posted = _clean_text(item.get("date_posted"))
        if not company or not role or source not in {"linkedin", "indeed"}:
            continue

        key = (company.lower(), role.lower(), location.lower(), source)
        if key in seen_jobs:
            continue

        seen_jobs.add(key)
        deduped.append(
            {
                "company": company,
                "role": role,
                "location": location,
                "date_posted": date_posted,
                "source": source,
            }
        )

    return deduped


async def discover_companies_from_jobs(role: str, max_pages: int = 3) -> list[dict[str, str]]:
    role_query = _clean_text(role)
    if not role_query:
        return []

    pages = max(1, min(max_pages, 10))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
        )

        try:
            linkedin_jobs = await _scrape_linkedin_jobs(context, role_query, pages)
            if linkedin_jobs:
                return _dedupe_jobs(linkedin_jobs)

            indeed_jobs = await _scrape_indeed_jobs(context, role_query, pages)
            if indeed_jobs:
                return _dedupe_jobs(indeed_jobs)

            return []
        finally:
            await context.close()
            await browser.close()
