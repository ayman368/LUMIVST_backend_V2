import asyncio
from playwright.async_api import async_playwright

async def get_real_url():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = await context.new_page()
        print("Navigating to home...")
        await page.goto("https://www.saudiexchange.sa/wps/portal/saudiexchange/home/")
        await page.wait_for_load_state("domcontentloaded")
        
        print("Searching for 1120...")
        # Often the search input has id="companySymbol" or similar. We can type into it.
        # Let's just find the search input
        await page.evaluate("""() => {
            const inputs = document.querySelectorAll('input[type="text"], input[type="search"]');
            for(let i of inputs) {
                if(i.placeholder.toLowerCase().includes('search') || i.id.toLowerCase().includes('search')) {
                    i.value = '1120';
                    i.dispatchEvent(new Event('input', { bubbles: true }));
                    i.dispatchEvent(new Event('change', { bubbles: true }));
                    break;
                }
            }
        }""")
        
        await page.wait_for_timeout(3000)
        # Try to click the first suggestion
        await page.evaluate("""() => {
            const links = document.querySelectorAll('a');
            for(let l of links) {
                if(l.innerText.includes('1120') || l.href.includes('1120')) {
                    console.log("Clicking", l.href);
                    l.click();
                    break;
                }
            }
        }""")
        await page.wait_for_timeout(5000)
        print("Final URL:", page.url)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(get_real_url())
