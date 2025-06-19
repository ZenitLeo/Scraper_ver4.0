from typing import Dict, List
from playwright.async_api import Page

class FacebookDomAnalyzer:
    async def analyze_feed_posts(self, page: Page) -> Dict[str, List[str]]:
        """
        Анализирует главную ленту или группу, находит контейнер с постами, кнопки комментариев, комменты, реплаи.
        """
        # ---- Анализируем ленту (основная страница или группа) ----
        candidates = await page.query_selector_all('div[role="main"], div[data-pagelet]')
        best_selector = None
        best_count = 0

        for container in candidates:
            articles = await container.query_selector_all('div[role="article"]')
            count = len(articles)
            if count > best_count:
                best_count = count
                class_name = await container.get_attribute("class")
                if class_name:
                    best_selector = f'div.{class_name.replace(" ", ".")} div[role="article"]'
                else:
                    best_selector = 'div[role="article"]'

        # ---- Поиск кнопок комментариев ----
        comment_button_selectors = []
        comment_btns = await page.query_selector_all('span, div[role="button"], a')
        for btn in comment_btns:
            try:
                text = (await btn.inner_text()).lower()
                if "comment" in text or "коммент" in text or "ответ" in text:
                    btn_class = await btn.get_attribute("class")
                    if btn_class:
                        sel = f'.{btn_class.replace(" ", ".")}:has-text("{text}")'
                        comment_button_selectors.append(sel)
            except Exception:
                continue
        if not comment_button_selectors:
            comment_button_selectors = [
                'div[role="button"]:has-text("Comment")',
                'span:has-text("comments")',
                'span:has-text("комментарий")'
            ]

        # ---- Поиск селекторов комментариев ----
        comment_selectors = []
        lists = await page.query_selector_all('ul[role="list"]')
        for ul in lists:
            li_articles = await ul.query_selector_all('li[role="article"]')
            if li_articles:
                ul_class = await ul.get_attribute("class")
                if ul_class:
                    sel = f'ul.{ul_class.replace(" ", ".")} li[role="article"]'
                else:
                    sel = 'ul[role="list"] li[role="article"]'
                comment_selectors.append(sel)
        if not comment_selectors:
            comment_selectors = [
                'ul[role="list"] li[role="article"]',
                'div[role="article"][tabindex="0"]'
            ]

        # ---- Поиск reply (ответы на комментарий) ----
        reply_selectors = []
        reply_btns = await page.query_selector_all('span, div[role="button"]')
        for btn in reply_btns:
            try:
                text = (await btn.inner_text()).lower()
                if "repl" in text or "ответ" in text:
                    btn_class = await btn.get_attribute("class")
                    if btn_class:
                        sel = f'.{btn_class.replace(" ", ".")}:has-text("{text}")'
                        reply_selectors.append(sel)
            except Exception:
                continue
        if not reply_selectors:
            reply_selectors = [
                'div[role="button"]:has-text("View replies")',
                'div[role="button"]:has-text("Показать ответы")',
                'span:has-text("ответ")'
            ]

        return {
            "post_selectors": [best_selector] if best_selector else ['div[role="article"]'],
            "comment_button_selectors": comment_button_selectors,
            "comment_selectors": comment_selectors,
            "reply_selectors": reply_selectors
        }

    async def analyze_modal(self, page: Page) -> Dict[str, List[str]]:
        """
        Анализирует открытое модальное окно поста (или комменты в модалке).
        Возвращает селекторы для контейнера, комментариев, кнопок "ещё", reply.
        """
        # ---- 1. Находим контейнер модалки ----
        modal_container = None
        modal_selectors = [
            'div[role="dialog"]',
            'div[aria-modal="true"]',
            'div[data-visualcompletion="ignore-dynamic-aria"][role="dialog"]',
            'div.x1n2onr6:has(div[role="main"])'
        ]
        for selector in modal_selectors:
            modal_container = await page.query_selector(selector)
            if modal_container:
                break

        # Если не нашли — возвращаем дефолтные селекторы
        if not modal_container:
            return {
                "modal_container_selector": modal_selectors,
                "modal_comment_selectors": [
                    'ul[role="list"] li[role="article"]',
                    'div[role="article"][tabindex="0"]'
                ],
                "modal_reply_selectors": [
                    'div[role="button"]:has-text("View replies")',
                    'span:has-text("ответ")'
                ],
                "modal_see_more_selectors": [
                    'div[role="button"]:has-text("See more")',
                    'div[role="button"]:has-text("Показать еще")'
                ]
            }

        # ---- 2. Ищем селекторы комментариев внутри модалки ----
        modal_comment_selectors = []
        lists = await modal_container.query_selector_all('ul[role="list"]')
        for ul in lists:
            li_articles = await ul.query_selector_all('li[role="article"]')
            if li_articles:
                ul_class = await ul.get_attribute("class")
                if ul_class:
                    sel = f'ul.{ul_class.replace(" ", ".")} li[role="article"]'
                else:
                    sel = 'ul[role="list"] li[role="article"]'
                modal_comment_selectors.append(sel)
        if not modal_comment_selectors:
            modal_comment_selectors = [
                'ul[role="list"] li[role="article"]',
                'div[role="article"][tabindex="0"]'
            ]

        # ---- 3. Кнопки "Посмотреть ещё" ----
        modal_see_more_selectors = []
        see_more_btns = await modal_container.query_selector_all('div[role="button"], span')
        for btn in see_more_btns:
            try:
                text = (await btn.inner_text()).lower()
                if "see more" in text or "показать еще" in text or "ещё" in text:
                    btn_class = await btn.get_attribute("class")
                    if btn_class:
                        sel = f'.{btn_class.replace(" ", ".")}:has-text("{text}")'
                        modal_see_more_selectors.append(sel)
            except Exception:
                continue
        if not modal_see_more_selectors:
            modal_see_more_selectors = [
                'div[role="button"]:has-text("See more")',
                'div[role="button"]:has-text("Показать еще")'
            ]

        # ---- 4. Кнопки reply (ответы) ----
        modal_reply_selectors = []
        reply_btns = await modal_container.query_selector_all('span, div[role="button"]')
        for btn in reply_btns:
            try:
                text = (await btn.inner_text()).lower()
                if "repl" in text or "ответ" in text:
                    btn_class = await btn.get_attribute("class")
                    if btn_class:
                        sel = f'.{btn_class.replace(" ", ".")}:has-text("{text}")'
                        modal_reply_selectors.append(sel)
            except Exception:
                continue
        if not modal_reply_selectors:
            modal_reply_selectors = [
                'div[role="button"]:has-text("View replies")',
                'span:has-text("ответ")'
            ]

        return {
            "modal_container_selector": [modal_selectors[0]],  # Основной селектор модалки
            "modal_comment_selectors": modal_comment_selectors,
            "modal_reply_selectors": modal_reply_selectors,
            "modal_see_more_selectors": modal_see_more_selectors,
        }

facebook_dom_analyzer = FacebookDomAnalyzer()
