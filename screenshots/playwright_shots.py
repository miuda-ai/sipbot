#!/usr/bin/env python3
import asyncio
from playwright.async_api import async_playwright

BASE = "http://127.0.0.1:8081"
OUT = "/tmp/ui-shots"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={"width": 1440, "height": 900})
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        await page.goto(BASE, wait_until="networkidle")
        await page.wait_for_timeout(800)

        # strategy modal checkbox row check
        await page.locator(".strategy-card .act-edit").first.click()
        await page.wait_for_timeout(500)
        box = page.locator("#f-announce-on")
        label = page.locator('label.inline', has=box)
        lb = await label.bounding_box()
        bb = await box.bounding_box()
        same_line = abs((bb["y"] + bb["height"]/2) - (lb["y"] + lb["height"]/2)) < 6
        print("[checkbox] same line:", same_line)
        await page.screenshot(path=f"{OUT}/v2-01-strategy-modal.png")
        await page.locator("#sm-close").click()
        await page.wait_for_timeout(200)

        # calls detail: two-pane signaling
        await page.locator('.tab[data-tab="calls"]').click()
        await page.wait_for_timeout(500)
        await page.locator("#call-table tbody tr").first.click()
        await page.wait_for_timeout(1200)
        await page.locator(".sig-row").nth(3).click()
        await page.wait_for_timeout(300)
        raw = await page.locator("#sig-raw").inner_text()
        print("[sig] right pane shows raw:", raw.startswith("SIP/2.0") or raw.startswith("INVITE") or len(raw) > 50)
        await page.screenshot(path=f"{OUT}/v2-02-call-detail.png", full_page=True)
        print("JS errors:", errors if errors else "none")
        await browser.close()

asyncio.run(main())
