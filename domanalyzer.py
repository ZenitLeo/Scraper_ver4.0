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
            ],
            "comment_button_selectors": [
                'div[role="button"]:has-text("Comment")',
                'span:has-text("comments")',
                'a[href*="comment_tracking"]'
            ],
            "comment_selectors": [
                'ul[role="list"] li[role="article"]',
                'div[role="article"][tabindex="0"]'
            ]
        }

    def _default_selectors(self):
        return {
            "post_selectors": [
                'div[data-pagelet="FeedUnit"]',
                'div[role="article"]:has(div[data-ad-preview="message"])',
            ],
            "comment_button_selectors": [
                'span:has-text("comments")',
                'div[role="button"]:has-text("Comment")'
            ],
            "comment_selectors": [
                'ul[role="list"] li[role="article"]',
                'div[role="article"][tabindex="0"]'
            ]
        }

facebook_dom_analyzer = FacebookDomAnalyzer()
