import asyncio
from playwright.async_api import async_playwright

async def test_urls():
    urls = [
        "https://www.saudiexchange.sa/wps/portal/saudiexchange/ourmarkets/main-market-watch/company-details/?companySymbol=1120",
        "https://www.saudiexchange.sa/wps/portal/tadawul/market-participants/issuers/issuers-directory/company-details/?companySymbol=1120",
        "https://www.saudiexchange.sa/wps/portal/saudiexchange/hidden/company-profile/?companySymbol=1120",
        "https://www.saudiexchange.sa/wps/portal/tadawul/home/company-profile/?companySymbol=1120"
    ]
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = await context.new_page()
        print("Establishing session at home page...")
        await page.goto("https://www.saudiexchange.sa/wps/portal/saudiexchange/home/", timeout=30000)
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(2000)
        
        for u in urls:
            try:
                print(f"\nTesting: {u}")
                await page.goto(u, timeout=30000)
                await page.wait_for_load_state("domcontentloaded")
                title = await page.title()
                print(f"Title: {title}")
                # Print some tabs
                tabs = await page.evaluate(r"""() => {
                    const els = document.querySelectorAll('ul.nav li, .tab, h2, h3, a');
                    return Array.from(els).map(e => e.innerText.trim()).filter(t => t.length > 0 && t.length < 30).slice(0, 5);
                }""")
                print(f"Sample links: {tabs}")
            except Exception as e:
                print(f"Failed: {e}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_urls())
