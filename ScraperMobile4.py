import json
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def setup_driver():
    options = Options()
    options.add_argument('--user-agent=Mozilla/5.0 (Linux; Android 11; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-logging')
    options.add_argument('--log-level=3')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_experimental_option('useAutomationExtension', False)
    return webdriver.Chrome(options=options)

def extract_post_id(mobile_url):
    match = re.search(r'/(\d+)/?$', mobile_url)
    return match.group(1) if match else None

    # Пример использования:
    sale_post_id = extract_post_id("https://m.facebook.com/BHPHSuccess/posts/not-our-data-but-i-have-a-friend-who-rejected-43-applications-last-month-in-may-/24775876965333872/")
    print(f"sale_post_id={sale_post_id}")  # sale_post_id=24775876965333872

def load_cookies(driver):
    driver.get('https://m.facebook.com')
    try:
        with open('facebook_cookies.json', 'r') as f:
            cookies = json.load(f)
        for cookie in cookies:
            driver.add_cookie(cookie)
        driver.refresh()
        print("Cookies загружены успешно")
    except FileNotFoundError:
        print("Файл facebook_cookies.json не найден. Убедитесь, что он существует.")
    except Exception as e:
        print(f"Ошибка загрузки cookies: {e}")

def get_post_links(driver):
    time.sleep(3)  # Ждем загрузки страницы
    
    # Сохраняем исходный URL для возврата
    original_url = driver.current_url
    print(f"Исходный URL: {original_url}")
    
    # Ищем кнопки комментариев для открытия постов
    comment_buttons = driver.find_elements(By.CSS_SELECTOR, 'div[aria-label="Leave a comment"]')
    
    if not comment_buttons:
        # Альтернативные селекторы
        alt_selectors = [
            'div[aria-label*="comment"]',
            'div[aria-label*="Comment"]',
            '[data-sigil="comment-inline-composer"]',
            'div[role="button"][aria-label*="Comment"]',
            'a[href*="/story.php"]',  # Прямые ссылки на посты
            'div[data-ft*="top_level_post_id"]'  # Контейнеры постов
        ]
        
        for selector in alt_selectors:
            comment_buttons = driver.find_elements(By.CSS_SELECTOR, selector)
            if comment_buttons:
                print(f"Найдено {len(comment_buttons)} элементов с селектором: {selector}")
                break
    else:
        print(f"Найдено {len(comment_buttons)} кнопок 'Leave a comment'")
    
    if not comment_buttons:
        print("Кнопки комментариев не найдены. Проверьте страницу группы.")
        print("Текущий URL:", driver.current_url)
        return []
    
    # Собираем ссылки на посты
    post_urls = []
    
    for i, button in enumerate(comment_buttons[:5]):  # Уменьшаем для тестирования
        try:
            print(f"Обрабатываем элемент #{i+1}")
            
            # Скроллим к элементу
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(1)
            
            # Проверяем, является ли элемент ссылкой на пост
            if button.tag_name == 'a' and button.get_attribute('href') and 'story.php' in button.get_attribute('href'):
                post_url = button.get_attribute('href')
                if post_url not in post_urls:
                    post_urls.append(post_url)
                    print(f"Найдена прямая ссылка на пост #{i+1}: {post_url}")
                continue
            
            # Ищем родительский контейнер поста для поиска ссылок
            try:
                post_container = button.find_element(By.XPATH, './ancestor::div[contains(@data-ft, "top_level_post_id") or contains(@class, "story")]')
                post_links = post_container.find_elements(By.CSS_SELECTOR, 'a[href*="/story.php"]')
                for link in post_links:
                    post_url = link.get_attribute('href')
                    if post_url and post_url not in post_urls:
                        post_urls.append(post_url)
                        print(f"Найдена ссылка в контейнере поста #{i+1}: {post_url}")
                        break
                if post_links:
                    continue
            except:
                pass
            
            # Попробуем кликнуть по элементу
            try:
                # Скроллим немного вверх чтобы избежать перекрытия
                driver.execute_script("window.scrollBy(0, -100);")
                time.sleep(1)
                
                # Сохраняем текущий URL перед кликом
                url_before_click = driver.current_url
                
                # Используем JavaScript клик
                driver.execute_script("arguments[0].click();", button)
                time.sleep(3)
                
                # Получаем URL после клика
                current_url = driver.current_url
                print(f"URL до клика: {url_before_click}")
                print(f"URL после клика: {current_url}")
                
                # Проверяем различные условия для определения поста
                if (current_url != url_before_click and 
                    (('story.php' in current_url) or 
                     ('posts/' in current_url) or 
                     ('permalink' in current_url) or
                     current_url != original_url)):
                    
                    if current_url not in post_urls:
                        post_urls.append(current_url)
                        print(f"Добавлен пост #{i+1}: {current_url}")
                
                # Возвращаемся к исходной странице
                if current_url != url_before_click:
                    driver.back()
                    time.sleep(3)
                    
            except Exception as e:
                print(f"Ошибка клика по элементу #{i+1}: {e}")
                # Попробуем вернуться к исходной странице
                try:
                    if driver.current_url != original_url:
                        driver.get(original_url)
                        time.sleep(3)
                except:
                    pass
                continue
                
        except Exception as e:
            print(f"Общая ошибка с элементом #{i+1}: {e}")
            continue
    
    print(f"Собрано {len(post_urls)} URL постов")
    return post_urls

def parse_post(driver, post_url):
    try:
        print(f"Загружаем пост: {post_url}")
        driver.get(post_url)
        time.sleep(5)  # Увеличиваем время ожидания
    except Exception as e:
        print(f"Ошибка загрузки поста {post_url}: {e}")
        return None
    
    post_data = {
        'post_url': post_url,
        'author_name': '',
        'author_url': '',
        'content': '',
        'comments': []
    }
    
    print(f"Текущий URL после загрузки поста: {driver.current_url}")
    
    try:
        # Ждем полной загрузки страницы
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "screen-root"))
        )
        
        # Автор поста - пробуем разные селекторы
        author_selectors = [
            '#screen-root > div > div:nth-child(3) > div:nth-child(3) > div > div.m.dtf > div.m.dtf > div:nth-child(2) > div:nth-child(1) > div > span.f6.a',
            'h3 a, strong a',
            '[data-ft*="top_level_post_id"] h3 a',
            'div[data-ft] h3 a',
            'span.f6.a',
            'h3 span',
            'strong span'
        ]
        
        for selector in author_selectors:
            try:
                author_elem = driver.find_element(By.CSS_SELECTOR, selector)
                if author_elem.text.strip():
                    post_data['author_name'] = author_elem.text.strip()
                    print(f"Автор найден с селектором '{selector}': {post_data['author_name']}")
                    
                    # Попробуем найти ссылку на автора
                    try:
                        if author_elem.tag_name == 'a':
                            post_data['author_url'] = author_elem.get_attribute('href')
                        else:
                            author_link = author_elem.find_element(By.XPATH, './/a')
                            post_data['author_url'] = author_link.get_attribute('href')
                    except:
                        pass
                    break
            except:
                continue
        
        if not post_data['author_name']:
            print("Автор поста не найден с помощью стандартных селекторов")
        
        # Контент поста - пробуем разные селекторы
        content_selectors = [
            '#screen-root > div > div:nth-child(3) > div:nth-child(4) > div > div > div > div > span',
            '[data-ft*="top_level_post_id"] div[data-sigil="m-story-dom-content"]',
            'div[data-sigil="m-story-dom-content"]',
            'div[data-ft] p',
            'div[data-ft] span',
            '[data-testid="post_message"]',
            'div[dir="auto"]'
        ]
        
        for selector in content_selectors:
            try:
                content_elem = driver.find_element(By.CSS_SELECTOR, selector)
                if content_elem.text.strip():
                    post_data['content'] = content_elem.text.strip()
                    print(f"Контент найден с селектором '{selector}': {post_data['content'][:100]}...")
                    break
            except:
                continue
        
        if not post_data['content']:
            print("Контент поста не найден с помощью стандартных селекторов")
        
        # Прокрутка страницы для загрузки комментариев
        print("Прокручиваем страницу для загрузки комментариев...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        
        # Попробуем кликнуть "Показать больше комментариев" если есть
        try:
            more_buttons = driver.find_elements(By.CSS_SELECTOR, 
                '[data-sigil="m-more-comments"], a[href*="comment"], div[role="button"]')
            for button in more_buttons[:3]:  # Ограничиваем количество попыток
                try:
                    if button.is_displayed() and ("comment" in button.text.lower() or "комментари" in button.text.lower()):
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(2)
                        print("Кликнули кнопку 'Показать больше комментариев'")
                except:
                    continue
        except:
            pass
        
        # Комментарии - пробуем разные подходы
        comments_found = 0
        
        # Метод 1: Используем ваши селекторы
        try:
            main_comments = driver.find_elements(By.CSS_SELECTOR, '#screen-root > div > div:nth-child(3) > div:nth-child(10) > div')
            print(f"Найдено {len(main_comments)} потенциальных контейнеров комментариев")
            
            for i, comment_container in enumerate(main_comments):
                try:
                    # Автор комментария
                    author_elem = comment_container.find_element(By.CSS_SELECTOR, 'div:nth-child(3) > div:nth-child(1) > div.m > div > div:nth-child(1) > div > span')
                    author_name = author_elem.text.strip()
                    
                    # Текст комментария
                    text_elem = comment_container.find_element(By.CSS_SELECTOR, 'div:nth-child(3) > div:nth-child(1) > div.m > div > div:nth-child(4)')
                    comment_text = text_elem.text.strip()
                    
                    if author_name and comment_text:
                        comment_data = {
                            'author': author_name,
                            'author_url': '',
                            'text': comment_text,
                            'type': 'comment'
                        }
                        
                        post_data['comments'].append(comment_data)
                        comments_found += 1
                        print(f"Комментарий {comments_found}: {author_name}")
                        
                except Exception:
                    # Пробуем альтернативные селекторы для этого контейнера
                    try:
                        # Альтернативные селекторы для автора комментария
                        alt_author_selectors = [
                            'h3 a', 'strong a', 'span a', 'div a'
                        ]
                        
                        # Альтернативные селекторы для текста комментария
                        alt_text_selectors = [
                            'div[data-sigil="comment-body"]',
                            'span[dir="auto"]',
                            'div[dir="auto"]'
                        ]
                        
                        author_name = ''
                        for auth_sel in alt_author_selectors:
                            try:
                                auth_elem = comment_container.find_element(By.CSS_SELECTOR, auth_sel)
                                if auth_elem.text.strip():
                                    author_name = auth_elem.text.strip()
                                    break
                            except:
                                continue
                        
                        comment_text = ''
                        for text_sel in alt_text_selectors:
                            try:
                                text_elem = comment_container.find_element(By.CSS_SELECTOR, text_sel)
                                if text_elem.text.strip():
                                    comment_text = text_elem.text.strip()
                                    break
                            except:
                                continue
                        
                        if author_name and comment_text:
                            comment_data = {
                                'author': author_name,
                                'author_url': '',
                                'text': comment_text,
                                'type': 'comment'
                            }
                            post_data['comments'].append(comment_data)
                            comments_found += 1
                            print(f"Комментарий {comments_found} (альт. метод): {author_name}")
                        
                    except:
                        continue
        except Exception as e:
            print(f"Ошибка поиска комментариев методом 1: {e}")
        
        # Метод 2: Общий поиск комментариев
        if comments_found == 0:
            try:
                all_comments = driver.find_elements(By.CSS_SELECTOR, '[data-sigil="comment"], div[data-ft*="comment"]')
                print(f"Найдено {len(all_comments)} комментариев общим методом")
                
                for comment in all_comments[:20]:  # Ограничиваем количество
                    try:
                        author_elem = comment.find_element(By.CSS_SELECTOR, 'h3 a, strong a, span a')
                        text_elem = comment.find_element(By.CSS_SELECTOR, '[data-sigil="comment-body"], span[dir="auto"], div[dir="auto"]')
                        
                        if author_elem.text.strip() and text_elem.text.strip():
                            comment_data = {
                                'author': author_elem.text.strip(),
                                'author_url': author_elem.get_attribute('href') if author_elem.tag_name == 'a' else '',
                                'text': text_elem.text.strip(),
                                'type': 'comment'
                            }
                            post_data['comments'].append(comment_data)
                            comments_found += 1
                            
                    except:
                        continue
            except Exception as e:
                print(f"Ошибка поиска комментариев методом 2: {e}")
        
        print(f"Всего найдено {comments_found} комментариев")
                
    except Exception as e:
        print(f"Общая ошибка парсинга поста {post_url}: {e}")
    
    return post_data

def main(group_url=None):
    driver = setup_driver()
    
    try:
        load_cookies(driver)
        
        # Переходим в группу или на главную
        if group_url:
            print(f"Переходим к группе: {group_url}")
            driver.get(group_url)
        else:
            driver.get('https://m.facebook.com')
        time.sleep(5)  # Увеличиваем время ожидания
        
        print(f"Текущий URL после загрузки: {driver.current_url}")
        print(f"Заголовок страницы: {driver.title}")
        
        # Проверяем, что мы успешно вошли
        if "login" in driver.current_url.lower() or "checkpoint" in driver.current_url.lower():
            print("Похоже, что не удалось войти в аккаунт. Проверьте cookies.")
            return
        
        # Попробуем прокрутить страницу, чтобы загрузить посты
        print("Прокручиваем страницу для загрузки постов...")
        for i in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
        
        all_posts_data = []
        
        # Получаем ссылки на посты
        post_links = get_post_links(driver)
        
        if not post_links:
            print("Посты не найдены. Возможно группа закрытая или нужна прокрутка страницы.")
            return
        
        for post_url in post_links:
            print(f"Парсинг поста: {post_url}")
            post_data = parse_post(driver, post_url)
            if post_data:
                all_posts_data.append(post_data)
                print(f"Спарсено: автор - {post_data['author_name']}, комментариев - {len(post_data['comments'])}")
            time.sleep(2)  # Пауза между постами
        
        # Сохраняем данные
        with open('facebook_posts.json', 'w', encoding='utf-8') as f:
            json.dump(all_posts_data, f, ensure_ascii=False, indent=2)
            
        print(f"Спарсено {len(all_posts_data)} постов")
        
    finally:
        driver.quit()

if __name__ == "__main__":
    # Укажите URL группы
    GROUP_URL = "https://m.facebook.com/groups/1075275215820713"
    main(GROUP_URL)
