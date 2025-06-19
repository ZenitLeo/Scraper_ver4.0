import asyncio
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, Browser, Page, Playwright
import pytz
import sys

class FacebookScraper:
    def __init__(self):
        self.current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.user = "Savka322"
        self.base_output_dir = Path("output")
        self.base_output_dir.mkdir(exist_ok=True)
        self.cookies_file = Path("facebook_cookies.json")
        
        self._setup_logging()
        self._init_data_structure()

    def _setup_logging(self) -> None:
        log_file = self.base_output_dir / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(log_file, encoding="utf-8"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _init_data_structure(self) -> None:
        self.data_structure = {
            "scraper_info": {
                "version": "4.0",
                "timestamp": self.current_date,
                "user": self.user
            },
            "posts": []
        }

    def validate_facebook_url(self, url: str) -> bool:
        """Валидация Facebook URL"""
        pattern = r"https?://(www\.)?(facebook|fb)\.com/.*"
        return bool(re.match(pattern, url))

    async def load_cookies(self, context) -> None:
        """Загружает cookies из файла перед запуском"""
        try:
            cookies_file = Path(r"C:\ScrappingVer3\facebook_cookies.json")
            if not cookies_file.exists():
                self.logger.warning(f"Cookie file not found at {cookies_file}, continuing without cookies")
                return

            with open(cookies_file, "r", encoding="utf-8") as f:
                cookie_data = json.load(f)

            # Fix for SameSite attribute
            for cookie in cookie_data["cookies"]:
                if "sameSite" in cookie and cookie["sameSite"] not in ["Strict", "Lax", "None"]:
                    cookie["sameSite"] = "Lax"
                elif "sameSite" not in cookie:
                    cookie["sameSite"] = "Lax"

            await context.add_cookies(cookie_data["cookies"])
            self.logger.info(f"Successfully loaded {len(cookie_data['cookies'])} cookies from {cookies_file}")

        except Exception as e:
            self.logger.error(f"Error loading cookies: {e}")

    async def wait_for_login(self, page: Page) -> bool:
        """Ожидание подтверждения логина от пользователя"""
        print("\n" + "="*50)
        print("\033[93mПожалуйста, убедитесь что вы залогинены в Facebook.")
        print("Проверьте страницу браузера.")
        print("После успешного логина, нажмите Enter для продолжения...\033[0m")
        print("="*50 + "\n")
        
        input("Нажмите Enter для продолжения...")
        
        try:
            # Улучшенные селекторы для проверки логина
            selectors_to_check = [
                'div[role="banner"]',
                '[data-pagelet="root"]',
                'div[data-pagelet="FeedUnit"]',
                'a[aria-label="Facebook"]',
                'div[aria-label="Home"]'
            ]
            
            for selector in selectors_to_check:
                try:
                    await page.wait_for_selector(selector, timeout=10000) # Увеличиваем таймаут
                    self.logger.info(f"Login confirmed - found selector: {selector}")
                    # Сохраняем куки сразу после успешного логина!
                    await self.save_cookies(page.context)
                    return True
                except:
                    continue
                    
            self.logger.warning("Could not confirm login with standard selectors, proceeding anyway")
            return False # Возвращаем False, если логин не подтвержден
            
        except Exception as e:
            self.logger.error(f"Login check failed: {e}")
            return False

    async def init_browser(self) -> tuple[Playwright, Browser, Page]:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-notifications',
                '--no-sandbox',
                '--disable-gpu',
                '--disable-infobars',
                '--disable-blink-features=AutomationControlled',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
        )
        
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        await self.load_cookies(context)
        page = await context.new_page()
        return playwright, browser, page

    async def scrape(self, url: str, max_posts: int = 50) -> None:
        if not self.validate_facebook_url(url):
            self.logger.error("Invalid Facebook URL provided")
            print("\033[91mОшибка: Некорректный URL Facebook\033[0m")
            return

        try:
            playwright, browser, page = await self.init_browser()
            
            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                self.logger.info("Page loaded successfully")

                if not await self.wait_for_login(page):
                    self.logger.error("Login verification failed!")
                    print("\033[91mОшибка: Не удалось подтвердить логин. Проверьте доступ к Facebook.\033[0m")
                    return

                print("\033[92mЛогин подтвержден успешно! Начинаем сбор данных...\033[0m")

                posts = await self.collect_posts_with_comments(page, max_posts)
                self.data_structure["posts"] = posts

                self.save_results()

            finally:
                await browser.close()
                await playwright.stop()

        except Exception as e:
            self.logger.error(f"Critical error: {e}")
            print(f"\033[91mКритическая ошибка: {e}\033[0m")
            raise

    async def collect_posts_with_comments(self, page: Page, max_posts: int) -> list:
        """Основной метод сбора постов с комментариями"""
        posts = []
        
        try:
            print("\033[94mНачинаем сбор постов с комментариями...\033[0m")
            
            # Прокручиваем страницу для загрузки контента
            for i in range(5):
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await page.wait_for_timeout(3000)
                print(f"\033[94mПрокрутка страницы: {i+1}/5\033[0m")

            # Более специфичные селекторы для постов (не комментариев)
            post_selectors = [
                'div[data-pagelet="FeedUnit"]',  # Основные посты в ленте
                'div[role="article"]:has(div[data-ad-preview="message"])',  # Посты с контентом
                'div.x1yztbdb:has(h3)',  # Посты с заголовками авторов
            ]
            
            post_elements = []
            for selector in post_selectors:
                elements = await page.query_selector_all(selector)
                if elements:
                    # Фильтруем, убираем комментарии
                    filtered_elements = []
                    for element in elements:
                        # Проверяем, что это не комментарий
                        element_text = await element.inner_text()
                        # Комментарии обычно короче и не содержат основную структуру поста
                        if len(element_text) > 100:  # Минимальная длина для поста
                            # Проверяем наличие структуры поста (автор + контент)
                            author_elements = await element.query_selector_all('h3 a, strong a')
                            if author_elements:
                                filtered_elements.append(element)
                    
                    if filtered_elements:
                        print(f"\033[94mНайдено {len(filtered_elements)} постов с селектором: {selector}\033[0m")
                        post_elements = filtered_elements
                        break
            
            if not post_elements:
                print("\033[91mНе удалось найти посты.\033[0m")
                return []

            total_posts = min(len(post_elements), max_posts)
            print(f"\033[94mОбрабатываем постов: {total_posts}\033[0m")

            for i, post_element in enumerate(post_elements[:max_posts]):
                print(f"\033[94mОбработка поста: {i+1}/{total_posts}\033[0m")
                
                try:
                    # Извлекаем базовую информацию из поста в ленте
                    post_data = await self.extract_basic_post_info(post_element)
                    
                    if post_data:
                        # Пытаемся открыть полную версию поста для получения комментариев
                        full_post_data = await self.open_post_and_extract_comments(page, post_element, post_data)
                        
                        if full_post_data:
                            posts.append(full_post_data)
                    
                    # Пауза между постами
                    await page.wait_for_timeout(2000)
                    
                except Exception as e:
                    self.logger.error(f"Error processing post {i+1}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Error collecting posts: {e}")
            print(f"\033[91mОшибка при сборе постов: {e}\033[0m")

        return posts

    async def extract_basic_post_info(self, post_element) -> dict:
        """Извлекаем базовую информацию из поста в ленте"""
        try:
            # Извлекаем URL поста - ИСПРАВЛЕНИЕ
            post_url = await self.extract_post_url(post_element)
            
            return {
                "id": await self.generate_post_id(post_element),
                "timestamp": datetime.now().isoformat(),
                "author": await self.extract_author_info(post_element),
                "content": await self.extract_content(post_element),
                "engagement": await self.extract_engagement_data(post_element),
                "post_url": post_url,  # ДОБАВЛЕНО: сохраняем URL поста
                "comments": [],
                "full_comments_extracted": False
            }
        except Exception as e:
            self.logger.error(f"Error extracting basic post info: {e}")
            return None
            
    async def extract_post_url(self, post_element) -> str:
        """Извлекаем прямую ссылку на пост"""
        try:
            # Различные селекторы для ссылки на пост
            url_selectors = [
                'a[href*="/posts/"]',
                'a[href*="story_fbid"]',
                'a[href*="/photo.php"]',
                'a[role="link"][aria-label*="ago"]',
                'a[role="link"][tabindex="0"]',
                # Ссылки на время публикации
                'a[role="link"] abbr',
                'abbr[data-utime] parent::a',
                # Альтернативные селекторы
                'h3 + div a[role="link"]',
                'div[data-ad-preview="message"] ~ div a[role="link"]'
            ]
            
            for selector in url_selectors:
                try:
                    if 'parent::' in selector:
                        # Специальная обработка для родительских элементов
                        abbr_element = await post_element.query_selector('abbr[data-utime]')
                        if abbr_element:
                            link_element = await abbr_element.evaluate('element => element.closest("a")')
                            if link_element:
                                href = await link_element.get_attribute('href')
                                if href and ('posts/' in href or 'story_fbid' in href):
                                    return self.normalize_facebook_url(href)
                    else:
                        link_elements = await post_element.query_selector_all(selector)
                        for link_element in link_elements:
                            href = await link_element.get_attribute('href')
                            if href and ('posts/' in href or 'story_fbid' in href or 'photo.php' in href):
                                return self.normalize_facebook_url(href)
                except Exception as e:
                    continue
            
            return "N/A"
        except Exception as e:
            self.logger.error(f"Error extracting post URL: {e}")
            return "N/A"
            
    def normalize_facebook_url(self, url: str) -> str:
        """Нормализуем Facebook URL"""
        try:
            if url.startswith('/'):
                return f"https://facebook.com{url}"
            elif not url.startswith('http'):
                return f"https://facebook.com/{url}"
            return url
        except:
            return url        

    async def open_post_and_extract_comments(self, page: Page, post_element, basic_post_data: dict) -> dict:
        """Открываем пост и извлекаем все комментарии"""
        try:
            # Ищем элементы с текстом "comments"
            comment_button_selectors = [
                'span.html-span:has-text("comments")',
                'span:has-text("comments")',
                'span.xdj266r:has-text("comments")',
                # Альтернативные селекторы
                'div[role="button"]:has-text("Comment")',
                'div[role="button"]:has-text("comment")',
                'a[role="link"][tabindex="0"]',
                'a[href*="/posts/"]',
                'a[href*="story_fbid"]'
            ]
            
            clicked_element = None
            for selector in comment_button_selectors:
                try:
                    # Ищем все элементы с данным селектором
                    elements = await post_element.query_selector_all(selector)
                    for element in elements:
                        text = await element.inner_text()
                        # Проверяем, что это элемент с "comments"
                        if 'comment' in text.lower():
                            clicked_element = element
                            print(f"\033[94mНайден элемент комментариев: '{text}'\033[0m")
                            break
                    if clicked_element:
                        break
                except Exception as e:
                    print(f"\033[93mОшибка поиска селектора {selector}: {e}\033[0m")
                    continue
            
            if not clicked_element:
                print(f"\033[93mНе найдена кнопка комментариев для поста {basic_post_data['id']}\033[0m")
                return basic_post_data
            
            # Получаем текущий URL для возврата
            current_url = page.url
            
            # Кликаем на элемент
            try:
                await clicked_element.click()
                print(f"\033[94mКлик по элементу комментариев для поста {basic_post_data['id']}\033[0m")
                
                # Ждем загрузки новой страницы или модального окна
                await page.wait_for_timeout(3000)
                
                # Проверяем, изменился ли URL (открылась новая страница)
                new_url = page.url
                if new_url != current_url:
                    print(f"\033[94mОткрылась новая страница поста: {new_url}\033[0m")
                    
                    # Извлекаем полные комментарии
                    comments = await self.extract_full_comments(page)
                    basic_post_data["comments"] = comments
                    basic_post_data["full_comments_extracted"] = True
                    basic_post_data["post_url"] = new_url
                    
                    print(f"\033[92mИзвлечено {len(comments)} комментариев для поста {basic_post_data['id']}\033[0m")
                    
                    # Возвращаемся назад
                    await page.go_back()
                    await page.wait_for_timeout(2000)
                    
                else:
                    # Открылось модальное окно
                    print(f"\033[94mОткрылось модальное окно\033[0m")
                    comments = await self.extract_full_comments(page)
                    basic_post_data["comments"] = comments
                    basic_post_data["full_comments_extracted"] = True
                    
                    print(f"\033[92mИзвлечено {len(comments)} комментариев для поста {basic_post_data['id']}\033[0m")
                    
                    # Улучшенное закрытие модального окна
                    await self.close_modal(page)
                
            except Exception as click_error:
                self.logger.error(f"Error clicking comment button: {click_error}")
                return basic_post_data
            
            return basic_post_data
            
        except Exception as e:
            self.logger.error(f"Error opening post for comments: {e}")
            return basic_post_data

    async def close_modal(self, page: Page) -> None:
        """Улучшенное закрытие модального окна"""
        try:
            print("\033[94mЗакрываем модальное окно...\033[0m")
            
            # Способ 1: ESC клавиша (самый надежный)
            try:
                await page.keyboard.press('Escape')
                await page.wait_for_timeout(2000)
                
                # Проверяем, закрылось ли модальное окно
                modals = await page.query_selector_all('div[role="dialog"]')
                if not modals:
                    print("\033[92mМодальное окно закрыто с помощью ESC\033[0m")
                    return
            except Exception as e:
                print(f"\033[93mESC не сработал: {e}\033[0m")
            
            # Способ 2: Кнопки закрытия
            close_selectors = [
                'div[aria-label="Close"]',
                'button[aria-label="Close"]',
                'div[role="button"][aria-label="Close"]',
                'div[aria-label="Закрыть"]',
                'button[aria-label="Закрыть"]',
                'svg[aria-label="Close"]',
                'div[role="button"] svg[viewBox="0 0 24 24"]',
                'i[data-visualcompletion="css-img"][style*="filter"]'
            ]
            
            for selector in close_selectors:
                try:
                    close_button = await page.query_selector(selector)
                    if close_button and await close_button.is_visible():
                        await close_button.click()
                        await page.wait_for_timeout(2000)
                        
                        # Проверяем результат
                        modals = await page.query_selector_all('div[role="dialog"]')
                        if not modals:
                            print(f"\033[92mМодальное окно закрыто кнопкой: {selector}\033[0m")
                            return
                except Exception as e:
                    continue
            
            # Способ 3: Клик вне модального окна
            try:
                # Кликаем в углу страницы
                await page.click('body', position={'x': 50, 'y': 50})
                await page.wait_for_timeout(1000)
                
                modals = await page.query_selector_all('div[role="dialog"]')
                if not modals:
                    print("\033[92mМодальное окно закрыто кликом вне области\033[0m")
                    return
            except Exception as e:
                print(f"\033[93mКлик вне области не сработал: {e}\033[0m")
            
            # Способ 4: Принудительное удаление модального окна (крайний случай)
            try:
                await page.evaluate('''
                    const modals = document.querySelectorAll('div[role="dialog"]');
                    modals.forEach(modal => modal.remove());
                ''')
                print("\033[93mМодальное окно удалено принудительно\033[0m")
            except Exception as e:
                print(f"\033[91mНе удалось принудительно закрыть модальное окно: {e}\033[0m")
                
        except Exception as e:
            self.logger.error(f"Error closing modal: {e}")


    async def extract_full_comments(self, page: Page) -> list:
        """Извлекаем все комментарии с полной страницы поста"""
        comments = []
        try:
            print("\033[94mИщем комментарии в модальном окне или на странице...\033[0m")
            await page.wait_for_timeout(3000)
            # Поиск контейнера с комментариями
            modal_selectors = [
                'div[role="dialog"]',
                'div[aria-modal="true"]',
                'div[data-visualcompletion="ignore-dynamic-aria"][role="dialog"]',
                'div.x1n2onr6:has(div[role="main"])'
            ]
            search_container = page
            for selector in modal_selectors:
                modal_container = await page.query_selector(selector)
                if modal_container:
                    search_container = modal_container
                    print(f"\033[94mНайдено модальное окно: {selector}\033[0m")
                    break

            # Новый приоритетный список селекторов
            comment_selectors = [
                'ul[role="list"] > li > div[role="article"]',
                'ul[role="list"] li[role="article"]',
                'div[role="article"][tabindex="0"]',
                # fallback — если ничего не найдено, ищем просто текстовые блоки
                'div[dir="auto"][style*="text-align"]'
            ]

            found_any = False
            for selector in comment_selectors:
                try:
                    comment_elements = await search_container.query_selector_all(selector)
                    print(f"DEBUG: Селектор {selector} нашёл {len(comment_elements)} элементов")
                    if comment_elements and len(comment_elements) > 0:
                        print(f"\033[94mНайдено {len(comment_elements)} комментариев с селектором: {selector}\033[0m")
                        found_any = True
                        for i, comment_element in enumerate(comment_elements):
                            try:
                                # Можно использовать твою extract_single_comment или кастомную
                                comment_data = await self.extract_single_comment(comment_element, i)
                                if comment_data and comment_data['content'].strip():
                                    comments.append(comment_data)
                            except Exception as e:
                                self.logger.error(f"Error extracting comment {i}: {e}")
                                continue
                        # Если нашли комментарии, прекращаем поиск по другим селекторам
                        if comments:
                            break
                except Exception as e:
                    self.logger.error(f"Error with selector {selector}: {e}")
                    continue

            if not found_any:
                print("\033[91mНе удалось найти комментарии ни одним селектором. Проверьте структуру страницы!\033[0m")
            else:
                print(f"\033[92mВсего извлечено уникальных комментариев: {len(comments)}\033[0m")

        except Exception as e:
            self.logger.error(f"Error extracting full comments: {e}")
            print(f"\033[91mОшибка при извлечении комментариев: {e}\033[0m")
        return comments
        
    async def extract_single_comment(self, comment_element, index: int) -> dict:
        """Извлекаем данные одного комментария"""
        try:
            # ИСПРАВЛЕННЫЕ селекторы для автора комментария
            author_selectors = [
                # Точные селекторы для имени автора в ссылке
                'h3 a span',
                'strong a span', 
                'a[role="link"] strong',
                'a[role="link"] span.x3nfvch5',
                'a[href*="/user/"] strong',
                'a[href*="/profile.php"] strong',
                # Альтернативные селекторы
                'div[role="article"] h3 a',
                'div[role="article"] strong a',
                # Для случаев без ссылки
                'h3 strong',
                'span.x193iq5w.xeuugli.x13faqss.x1vvkbs'
            ]
            
            author = "N/A"
            for selector in author_selectors:
                try:
                    author_element = await comment_element.query_selector(selector)
                    if author_element:
                        author_text = await author_element.inner_text()
                        # Проверяем, что это действительно имя (не пустое и не слишком длинное)
                        if author_text and author_text.strip() and len(author_text.strip()) < 100:
                            author = author_text.strip()
                            print(f"\033[94mНайден автор '{author}' с селектором: {selector}\033[0m")
                            break
                except Exception as e:
                    continue
            
            # ИСПРАВЛЕННЫЕ селекторы для содержимого комментария
            # Важно: ищем контент ПОСЛЕ автора, исключая элементы с именем автора
            content_selectors = [
                # Основные селекторы для текста комментария
                'div[data-testid="comment-content"] span[dir="auto"]',
                'div[data-testid="UFI2Comment/body"] span[dir="auto"]',
                
                # Структурные селекторы - ищем span с текстом, но не в заголовке
                'div[role="article"] > div > div:not(:first-child) span[dir="auto"]',
                'div[role="article"] div:not(h3):not(strong) span[dir="auto"]',
                
                # Селекторы для основного контента
                'div.x1iorvi4.x1pi3gq7 span[dir="auto"]',
                'div.xdj266r.x11i5rnm.xat24cr span[dir="auto"]',
                
                # Широкие селекторы с фильтрацией
                'span[dir="auto"]'
            ]
            
            content = ""
            for selector in content_selectors:
                try:
                    if selector == 'span[dir="auto"]':
                        # Для широкого селектора применяем дополнительную фильтрацию
                        span_elements = await comment_element.query_selector_all(selector)
                        for span_element in span_elements:
                            span_text = await span_element.inner_text()
                            if span_text and span_text.strip():
                                # Проверяем, что это не имя автора
                                if span_text.strip() != author and len(span_text.strip()) > 3:
                                    # Проверяем, что элемент не находится в заголовке
                                    parent_h3 = await span_element.evaluate('element => element.closest("h3")')
                                    parent_strong = await span_element.evaluate('element => element.closest("strong")')
                                    parent_link = await span_element.evaluate('element => element.closest("a[role=\\"link\\"]")')
                                    
                                    if not parent_h3 and not parent_strong and not parent_link:
                                        content = span_text.strip()
                                        print(f"\033[94mНайден контент '{content[:50]}...' с широким селектором\033[0m")
                                        break
                    else:
                        content_element = await comment_element.query_selector(selector)
                        if content_element:
                            content_text = await content_element.inner_text()
                            if content_text and content_text.strip() and content_text.strip() != author:
                                content = content_text.strip()
                                print(f"\033[94mНайден контент '{content[:50]}...' с селектором: {selector}\033[0m")
                                break
                except Exception as e:
                    continue
            
            # Если контент все еще пустой, попробуем альтернативный подход
            if not content or content == author:
                try:
                    # Получаем весь текст элемента и пытаемся выделить контент
                    full_text = await comment_element.inner_text()
                    if full_text and author != "N/A":
                        # Убираем имя автора из начала текста
                        lines = full_text.split('\n')
                        content_lines = []
                        author_found = False
                        
                        for line in lines:
                            line = line.strip()
                            if not line:
                                continue
                            # Пропускаем строку с именем автора
                            if line == author and not author_found:
                                author_found = True
                                continue
                            # Пропускаем временные метки и служебную информацию
                            if any(keyword in line.lower() for keyword in ['ago', 'hours', 'minutes', 'like', 'reply', 'edited']):
                                continue
                            content_lines.append(line)
                        
                        if content_lines:
                            content = ' '.join(content_lines)
                            print(f"\033[94mИзвлечен контент альтернативным методом: '{content[:50]}...'\033[0m")
                            
                except Exception as e:
                    print(f"\033[93mОшибка альтернативного извлечения контента: {e}\033[0m")
            
            # Селекторы для времени комментария
            timestamp_selectors = [
                'a[role="link"] abbr',
                'abbr[data-utime]',
                'time',
                'a.x1i10h5g abbr',
                'span.x4k7w5x.x1h91t0o.x1beo9mf.xaig5y3.x2lwn1j.xmkpm',
                'a[href*="comment_id"] abbr'
            ]
            
            timestamp = datetime.now().isoformat()
            for selector in timestamp_selectors:
                try:
                    timestamp_element = await comment_element.query_selector(selector)
                    if timestamp_element:
                        # Пытаемся получить title или текст
                        title = await timestamp_element.get_attribute('title')
                        if title:
                            timestamp = title
                            break
                        else:
                            text = await timestamp_element.inner_text()
                            if text and 'ago' in text.lower():
                                timestamp = text
                                break
                except:
                    continue
            
            # Проверяем качество извлеченных данных
            if content and content != author and len(content.strip()) > 0:
                result = {
                    "id": f"comment_{index}_{hash(f'{author}_{content}')}"[:20],
                    "author": author,
                    "content": content,
                    "timestamp": timestamp,
                    "extracted_at": datetime.now().isoformat()
                }
                print(f"\033[92mУспешно извлечен комментарий: автор='{author}', контент='{content[:30]}...'\033[0m")
                return result
            else:
                print(f"\033[93mПроблема с извлечением: автор='{author}', контент='{content}'\033[0m")
                return None
        except Exception as e:
            self.logger.error(f"Error extracting single comment: {e}")
            return None


    
    async def load_more_comments(self, page: Page, container) -> None:
        """Загружаем дополнительные комментарии"""
        try:
            # Прокручиваем контейнер
            scroll_attempts = 8
            for i in range(scroll_attempts):
                if container != page:
                    # Прокрутка внутри модального окна
                    await container.evaluate('element => element.scrollTo(0, element.scrollHeight)')
                else:
                    # Прокрутка страницы
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                
                await page.wait_for_timeout(2000)
                print(f"\033[94mПрокрутка для загрузки комментариев: {i+1}/{scroll_attempts}\033[0m")
            
            # Ищем и кликаем кнопки "View more comments"
            load_more_selectors = [
                'div[role="button"]:has-text("View more comments")',
                'div[role="button"]:has-text("Show more comments")',
                'span:has-text("View more comments")',
                'a[role="button"]:has-text("View more comments")',
                'div[role="button"]:has-text("Показать больше комментариев")',
                'span:has-text("Показать больше комментариев")',
                'div[role="button"][tabindex="0"]:has-text("comment")',
                'div[role="button"][aria-label*="more comment"]',
                'div[role="button"][aria-expanded="false"]'
            ]
            
            for attempt in range(3):  # Пытаемся несколько раз
                clicked = False
                for selector in load_more_selectors:
                    try:
                        load_button = await container.query_selector(selector)
                        if load_button:
                            # Проверяем видимость кнопки
                            is_visible = await load_button.is_visible()
                            if is_visible:
                                await load_button.click()
                                await page.wait_for_timeout(3000)
                                print(f"\033[94mНажата кнопка загрузки комментариев: {selector}\033[0m")
                                clicked = True
                                break
                    except Exception as e:
                        continue
                
                if not clicked:
                    break
                    
        except Exception as e:
            self.logger.error(f"Error loading more comments: {e}")
            
    async def extract_comment_replies(self, comment_element) -> list:
        """Извлекаем ответы на комментарий"""
        replies = []
        try:
            # Ищем кнопку "View replies" или "Show replies"
            reply_buttons = [
                'div[role="button"]:has-text("View")',
                'div[role="button"]:has-text("replies")',
                'div[role="button"]:has-text("reply")',
                'span:has-text("View")',
                'span:has-text("replies")'
            ]
            
            for selector in reply_buttons:
                try:
                    button = await comment_element.query_selector(selector)
                    if button:
                        button_text = await button.inner_text()
                        if any(word in button_text.lower() for word in ['view', 'show', 'replies', 'reply']):
                            await button.click()
                            await asyncio.sleep(2)
                            print(f"\033[94mНажата кнопка просмотра ответов: {button_text}\033[0m")
                            break
                except:
                    continue
            
            # Ищем элементы ответов
            reply_selectors = [
                'div[role="article"][aria-label*="Reply"]',
                'li[data-testid="comment-list-item"] div[role="article"]',
                'ul[role="list"] > li > div[role="article"]'
            ]
            
            for selector in reply_selectors:
                try:
                    reply_elements = await comment_element.query_selector_all(selector)
                    if reply_elements:
                        print(f"\033[94mНайдено {len(reply_elements)} ответов\033[0m")
                        for i, reply_element in enumerate(reply_elements):
                            reply_data = await self.extract_single_comment(reply_element, f"reply_{i}")
                            if reply_data and reply_data['content'].strip():
                                replies.append(reply_data)
                        break
                except:
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error extracting comment replies: {e}")
        
        return replies
            
       
        
    async def generate_post_id(self, post_element) -> str:
        """Генерируем уникальный ID для поста"""
        try:
            # Попытка извлечь URL поста
            link_element = await post_element.query_selector('a[href*="/posts/"]') or \
                           await post_element.query_selector('a[href*="story_fbid"]')
            if link_element:
                href = await link_element.get_attribute('href')
                if href:
                    match = re.search(r'story_fbid=(\d+)', href) or \
                            re.search(r'/posts/(\d+)', href)
                    if match:
                        return match.group(1)
            
            # Если URL не найден, генерируем ID на основе хэша контента
            content = await post_element.inner_text()
            return str(hash(content))[:10]
        except Exception as e:
            self.logger.warning(f"Could not generate post ID from URL, falling back to content hash: {e}")
            content = await post_element.inner_text()
            return str(hash(content))[:10]

    async def extract_author_info(self, post_element) -> dict:
        """Извлекаем информацию об авторе поста"""
        try:
            author_name_element = await post_element.query_selector('h3 a') or \
                                  await post_element.query_selector('strong a') or \
                                  await post_element.query_selector('span.x3nfch5')
            author_name = await author_name_element.inner_text() if author_name_element else "N/A"

            author_url_element = await post_element.query_selector('h3 a') or \
                                 await post_element.query_selector('strong a')
            author_url = await author_url_element.get_attribute('href') if author_url_element else "N/A"

            return {"name": author_name, "url": author_url}
        except Exception as e:
            self.logger.error(f"Error extracting author info: {e}")
            return {"name": "N/A", "url": "N/A"}

    async def extract_content(self, post_element) -> str:
        """Извлекаем текстовое содержимое поста"""
        try:
            content_element = await post_element.query_selector('div[data-ad-preview="message"]') or \
                              await post_element.query_selector('div.x1iorvi4.x1pi3gq7.x1swvt13.x1iorvi4.x10ml5cb')
            content = await content_element.inner_text() if content_element else ""
            
            # Разворачиваем "Показать еще"
            show_more_button = await post_element.query_selector('div[role="button"]:has-text("Показать еще")') or \
                               await post_element.query_selector('div[role="button"]:has-text("See more")')
            if show_more_button:
                await show_more_button.click()
                await page.wait_for_timeout(1000)  # ИСПРАВЛЕНО: используем page вместо post_element
                content = await content_element.inner_text() if content_element else content

            return content.strip()
        except Exception as e:
            self.logger.error(f"Error extracting content: {e}")
            return ""

    async def extract_engagement_data(self, post_element) -> dict:
        """Извлекаем данные о вовлеченности (лайки, комментарии, репосты)"""
        try:
            likes_element = await post_element.query_selector('span[data-testid="UFI2ReactionCount/root"]') or \
                            await post_element.query_selector('div.x1qjc9v5.x1q0g3np.x1qughib.x1s65ae0.x12nagc')
            likes = await likes_element.inner_text() if likes_element else "0"

            comments_element = await post_element.query_selector('span:has-text("comments")') or \
                              await post_element.query_selector('span:has-text("comment")')
            comments = await comments_element.inner_text() if comments_element else "0"

            shares_element = await post_element.query_selector('span:has-text("shares")') or \
                            await post_element.query_selector('span:has-text("share")')
            shares = await shares_element.inner_text() if shares_element else "0"

            return {
                "likes": self.extract_number_from_text(likes),
                "comments": self.extract_number_from_text(comments),
                "shares": self.extract_number_from_text(shares)
            }
        except Exception as e:
            self.logger.error(f"Error extracting engagement data: {e}")
            return {"likes": 0, "comments": 0, "shares": 0}

    def extract_number_from_text(self, text: str) -> int:
        """Извлекаем числовое значение из текста (например, '25 comments' -> 25)"""
        try:
            # Ищем числа в тексте
            numbers = re.findall(r'\d+', text.replace(',', '').replace('.', ''))
            if numbers:
                return int(numbers[0])
            return 0
        except Exception as e:
            self.logger.error(f"Error extracting number from text '{text}': {e}")
            return 0

    async def extract_content(self, post_element) -> str:
        """Извлекаем текстовое содержимое поста"""
        try:
            content_element = await post_element.query_selector('div[data-ad-preview="message"]') or \
                              await post_element.query_selector('div.x1iorvi4.x1pi3gq7.x1swvt13.x1iorvi4.x10ml5cb')
            content = await content_element.inner_text() if content_element else ""
            
            # Разворачиваем "Показать еще" - ИСПРАВЛЕНО: убираем page.wait_for_timeout
            show_more_button = await post_element.query_selector('div[role="button"]:has-text("Показать еще")') or \
                               await post_element.query_selector('div[role="button"]:has-text("See more")')
            if show_more_button:
                await show_more_button.click()
                # Используем asyncio.sleep вместо page.wait_for_timeout
                await asyncio.sleep(1)
                content = await content_element.inner_text() if content_element else content

            return content.strip()
        except Exception as e:
            self.logger.error(f"Error extracting content: {e}")
            return ""

    def save_results(self) -> None:
        """Сохраняем результаты с подробной статистикой"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = self.base_output_dir / f"facebook_scrape_{timestamp}.json"
            
            # Добавляем статистику
            self.data_structure["statistics"] = {
                "total_posts": len(self.data_structure['posts']),
                "posts_with_comments": sum(1 for post in self.data_structure['posts'] if post.get('full_comments_extracted', False)),
                "total_comments": sum(len(post.get('comments', [])) for post in self.data_structure['posts']),
                "posts_with_urls": sum(1 for post in self.data_structure['posts'] if post.get('post_url', 'N/A') != 'N/A'),
                "scraping_duration": "calculated_in_scraper"
            }
            
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(self.data_structure, f, ensure_ascii=False, indent=2)
            
            # Выводим подробную статистику
            stats = self.data_structure["statistics"]
            print(f"\033[92m{'='*50}\033[0m")
            print(f"\033[92mРезультаты сохранены в: {filename}\033[0m")
            print(f"\033[92mОбщая статистика:\033[0m")
            print(f"\033[92m├─ Всего постов: {stats['total_posts']}\033[0m")
            print(f"\033[92m├─ Постов с комментариями: {stats['posts_with_comments']}\033[0m")
            print(f"\033[92m├─ Всего комментариев: {stats['total_comments']}\033[0m")
            print(f"\033[92m└─ Постов с URL: {stats['posts_with_urls']}\033[0m")
            print(f"\033[92m{'='*50}\033[0m")
            
            # Показываем примеры собранных данных
            if self.data_structure['posts']:
                first_post = self.data_structure['posts'][0]
                print(f"\033[94mПример собранных данных:\033[0m")
                print(f"\033[94m├─ Автор: {first_post.get('author', {}).get('name', 'N/A')}\033[0m")
                print(f"\033[94m├─ URL поста: {first_post.get('post_url', 'N/A')}\033[0m")
                print(f"\033[94m└─ Комментариев: {len(first_post.get('comments', []))}\033[0m")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
            print(f"\033[91mОшибка при сохранении: {e}\033[0m")

    async def save_cookies(self, context) -> None:
        """Сохраняем cookies в файл после логина"""
        try:
            cookies = await context.cookies()
            cookie_data = {
                "timestamp": datetime.now().isoformat(),
                "cookies": cookies
            }
            cookies_path = Path(r"C:\ScrappingVer3\facebook_cookies.json")
            with open(cookies_path, "w", encoding="utf-8") as f:
                json.dump(cookie_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Cookies saved to: {cookies_path}")
        except Exception as e:
            self.logger.error(f"Error saving cookies: {e}")

def main():
    """Основная функция для запуска скрапера"""
    print("\033[94m" + "="*60)
    print("Facebook Scraper v4.0")  
    print("="*60 + "\033[0m")
    
    try:
        url = input("\033[93mВведите URL Facebook страницы или поста: \033[0m").strip()
        if not url:
            print("\033[91mОшибка: URL не может быть пустым\033[0m")
            return
            
        try:
            max_posts = int(input("\033[93mМаксимальное количество постов для сбора (по умолчанию 10): \033[0m") or "10")
        except ValueError:
            max_posts = 10
            print("\033[93mИспользуется значение по умолчанию: 10 постов\033[0m")
        
        scraper = FacebookScraper()
        
        print(f"\033[94mНачинаем скрапинг: {url}\033[0m")
        print(f"\033[94mМаксимум постов: {max_posts}\033[0m")
        
        # Запускаем асинхронный скрапинг
        asyncio.run(scraper.scrape(url, max_posts))
        
        print("\033[92mСкрапинг завершен успешно!\033[0m")
        
    except KeyboardInterrupt:
        print("\n\033[93mСкрапинг прерван пользователем\033[0m")
    except Exception as e:
        print(f"\033[91mКритическая ошибка: {e}\033[0m")
        logging.error(f"Critical error in main: {e}")

if __name__ == "__main__":
    main()
