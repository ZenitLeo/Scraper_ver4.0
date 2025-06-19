from typing import Dict, List
from playwright.async_api import Page

class FacebookDomAnalyzer:
    def __init__(self):
        pass

    async def get_selectors(self, page: Page) -> Dict[str, List[str]]:
        """
        Анализирует DOM и возвращает подходящие селекторы
        для текущей страницы (группа, лента, модалка).
        """
        url = page.url
        if "/groups/" in url:
            return self._group_selectors()
        # можно добавить автоопределение типа страницы по DOM
        return self._default_selectors()

    def _group_selectors(self):
        return {
            "post_selectors": [
                'div[data-pagelet^="GroupFeed"] div[role="article"]',
                'div[data-pagelet^="FeedUnit"]',
                'div[role="feed"] > div[data-pagelet^="FeedUnit"]'
            ],
            "comment_button_selectors": [
                'div[role="button"]:has-text("Comment")',
                'span:has-text("comments")',
                'a[href*="comment_tracking"]',
                'div[aria-label="Comment"]'
            ],
            "comment_selectors": [
                'ul[role="list"] li[role="article"]',
                'div[role="article"][tabindex="0"]',
                'div[data-testid="UFI2Comment/root"]'
            ],
            "reply_buttons": [
                'div[role="button"]:has-text("View replies")',
                'div[role="button"]:has-text("Показать ответы")',
                'span:has-text("ответ")',
                'span:has-text("repl")',
                'span:has-text("replies")',
                'div[role="button"]:has-text("View more replies")'
            ],
            "author_selectors": [
                'h3 a span',
                'strong a span', 
                'a[role="link"] strong',
                'a[role="link"] span.x3nfvch5',
                'a[href*="/user/"] strong',
                'a[href*="/profile.php"] strong',
                'div[role="article"] h3 a',
                'div[role="article"] strong a',
                'h3 strong',
                'span.x193iq5w.xeuugli.x13faqss.x1vvkbs',
                'a[data-hovercard]' # General selector for author links
            ],
            "content_selectors": [
                'div[data-testid="comment-content"] span[dir="auto"]',
                'div[data-testid="UFI2Comment/body"] span[dir="auto"]',
                'div[role="article"] > div > div:not(:first-child) span[dir="auto"]',
                'div[role="article"] div:not(h3):not(strong) span[dir="auto"]',
                'div.x1iorvi4.x1pi3gq7 span[dir="auto"]',
                'div.xdj266r.x11i5rnm.xat24cr span[dir="auto"]',
                'span[dir="auto"]',
                'div[data-testid="post_message"] span[dir="auto"]' # For post content
            ]
        }
    async def analyze_feed_posts(self, page):
        """Синоним get_selectors для совместимости с новым кодом"""
        return await self.get_selectors(page)

    def _default_selectors(self):
        return {
            "post_selectors": [
                'div[data-pagelet="FeedUnit"]',
                'div[role="article"]:has(div[data-ad-preview="message"])',
                'div[role="feed"] > div[data-pagelet^="FeedUnit"]'
            ],
            "comment_button_selectors": [
                'span:has-text("comments")',
                'div[role="button"]:has-text("Comment")',
                'div[aria-label="Comment"]'
            ],
            "comment_selectors": [
                'ul[role="list"] li[role="article"]',
                'div[role="article"][tabindex="0"]',
                'div[data-testid="UFI2Comment/root"]'
            ],
            "reply_buttons": [
                'div[role="button"]:has-text("View replies")',
                'div[role="button"]:has-text("Показать ответы")',
                'span:has-text("ответ")',
                'span:has-text("repl")',
                'span:has-text("replies")',
                'div[role="button"]:has-text("View more replies")'
            ],
            "author_selectors": [
                'h3 a span',
                'strong a span', 
                'a[role="link"] strong',
                'a[role="link"] span.x3nfvch5',
                'a[href*="/user/"] strong',
                'a[href*="/profile.php"] strong',
                'div[role="article"] h3 a',
                'div[role="article"] strong a',
                'h3 strong',
                'span.x193iq5w.xeuugli.x13faqss.x1vvkbs',
                'a[data-hovercard]' # General selector for author links
            ],
            "content_selectors": [
                'div[data-testid="comment-content"] span[dir="auto"]',
                'div[data-testid="UFI2Comment/body"] span[dir="auto"]',
                'div[role="article"] > div > div:not(:first-child) span[dir="auto"]',
                'div[role="article"] div:not(h3):not(strong) span[dir="auto"]',
                'div.x1iorvi4.x1pi3gq7 span[dir="auto"]',
                'div.xdj266r.x11i5rnm.xat24cr span[dir="auto"]',
                'span[dir="auto"]',
                'div[data-testid="post_message"] span[dir="auto"]' # For post content
            ]
        }

facebook_dom_analyzer = FacebookDomAnalyzer()
