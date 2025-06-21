import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, lru_cache
from cachetools import TTLCache, LRUCache
import multiprocessing
import psutil
import GPUtil
import time
import json
from datetime import datetime, timedelta
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import weakref
import gc
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
import threading
from queue import Queue, Empty, Full
import asyncio
from urllib.parse import urljoin, urlparse
import hashlib
import pickle
from contextlib import contextmanager
import traceback
from enum import Enum

# Настройка логирования
class LogLevel(Enum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

@dataclass
class ScrapingConfig:
    """Конфигурация для скрапинга"""
    group_url: str
    cookies_file: str = "cookies.json"
    max_posts: int = 5
    batch_size: int = 5
    max_scroll_attempts: int = 400
    scroll_delay: float = 2.0
    page_load_timeout: int = 30
    implicit_wait: int = 5
    output_dir: str = "output"
    log_level: LogLevel = LogLevel.INFO
    enable_gpu: bool = True
    parallel_workers: int = 4
    cache_size: int = 1000
    cache_ttl: int = 3600
    retry_attempts: int = 3
    retry_delay: float = 1.0

@dataclass
class AuthorInfo:
    """Информация об авторе поста"""
    name: str
    profile_url: Optional[str] = None
    avatar_url: Optional[str] = None
    is_verified: bool = False
    join_date: Optional[str] = None

@dataclass
class CommentInfo:
    """Расширенная информация о комментарии"""
    author: AuthorInfo
    text: str
    posted_time: str
    scraped_time: str
    likes_count: int = 0
    replies_count: int = 0
    is_pinned: bool = False
    is_edited: bool = False
    replies: List['CommentInfo'] = None
    reactions: Dict[str, int] = None
    
    def __post_init__(self):
        if self.replies is None:
            self.replies = []
        if self.reactions is None:
            self.reactions = {}

@dataclass
class PostInfo:
    """Информация о посте"""
    author: AuthorInfo
    content: str
    posted_time: str
    post_url: str
    external_links: List[str]
    images: List[str]
    comments: List[CommentInfo]
    scraped_time: str
    likes_count: int = 0
    shares_count: int = 0
    reactions: Dict[str, int] = None
    post_type: str = "text"
    tags: List[str] = None
    
    def __post_init__(self):
        if self.reactions is None:
            self.reactions = {}
        if self.tags is None:
            self.tags = []

class LoggerManager:
    """Централизованное управление логированием"""
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка структурированного логирования"""
        logger = logging.getLogger('facebook_scraper')
        logger.setLevel(self.config.log_level.value)
        
        # Очищаем существующие хендлеры
        logger.handlers.clear()
        
        # Форматтер для логов
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        
        # Консольный хендлер
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.config.log_level.value)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Файловый хендлер с ротацией
        if not os.path.exists(self.config.output_dir):
            os.makedirs(self.config.output_dir)
            
        file_handler = RotatingFileHandler(
            os.path.join(self.config.output_dir, 'scraper.log'),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def log_performance(self, operation: str, duration: float, **kwargs):
        """Логирование производительности"""
        self.logger.info(f"PERFORMANCE: {operation} took {duration:.2f}s", extra=kwargs)
    
    def log_error_with_context(self, error: Exception, context: Dict[str, Any]):
        """Логирование ошибок с контекстом"""
        self.logger.error(
            f"ERROR: {str(error)}\nContext: {json.dumps(context, indent=2)}\n"
            f"Traceback: {traceback.format_exc()}"
        )

class RetryManager:
    """Управление повторными попытками"""
    
    def __init__(self, max_attempts: int = 3, delay: float = 1.0, backoff_factor: float = 2.0):
        self.max_attempts = max_attempts
        self.delay = delay
        self.backoff_factor = backoff_factor
        
    def retry(self, func):
        """Декоратор для повторных попыток"""
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = self.delay
            
            for attempt in range(self.max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < self.max_attempts - 1:
                        time.sleep(current_delay)
                        current_delay *= self.backoff_factor
                    
            raise last_exception
        return wrapper

class MemoryManager:
    """Управление памятью и ресурсами"""
    
    def __init__(self, logger: LoggerManager):
        self.logger = logger
        self.start_memory = psutil.virtual_memory().percent
        
    @contextmanager
    def memory_monitoring(self, operation_name: str):
        """Контекстный менеджер для мониторинга памяти"""
        start_memory = psutil.virtual_memory().percent
        start_time = time.time()
        
        try:
            yield
        finally:
            end_memory = psutil.virtual_memory().percent
            duration = time.time() - start_time
            memory_diff = end_memory - start_memory
            
            self.logger.log_performance(
                operation_name,
                duration,
                memory_change=f"{memory_diff:+.1f}%",
                final_memory=f"{end_memory:.1f}%"
            )
            
            # Принудительная очистка памяти при необходимости
            if memory_diff > 10:  # Если память выросла более чем на 10%
                gc.collect()
                self.logger.logger.warning(f"Force garbage collection after {operation_name}")

class CacheManager:
    """Улучшенное управление кэшированием"""
    
    def __init__(self, config: ScrapingConfig, logger: LoggerManager):
        self.config = config
        self.logger = logger
        
        # Различные типы кэшей
        self.url_cache = TTLCache(maxsize=config.cache_size, ttl=config.cache_ttl)
        self.selector_cache = LRUCache(maxsize=500)
        self.post_cache = TTLCache(maxsize=config.cache_size//2, ttl=config.cache_ttl*2)
        
        # Кэш для селекторов элементов
        self.element_cache = weakref.WeakKeyDictionary()
        
        # Статистика кэша
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0
        }
        
    def get_cached_element(self, driver, selector: str, timeout: int = 5):
        """Кэшированный поиск элементов"""
        cache_key = f"{selector}_{timeout}"
        
        if cache_key in self.selector_cache:
            self.cache_stats['hits'] += 1
            return self.selector_cache[cache_key]
        
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            self.selector_cache[cache_key] = element
            self.cache_stats['misses'] += 1
            return element
        except TimeoutException:
            return None
            
    def cache_post_data(self, post_url: str, data: PostInfo):
        """Кэширование данных поста"""
        self.post_cache[post_url] = data
        
    def get_cached_post(self, post_url: str) -> Optional[PostInfo]:
        """Получение кэшированных данных поста"""
        return self.post_cache.get(post_url)
        
    def clear_cache(self):
        """Очистка всех кэшей"""
        self.url_cache.clear()
        self.selector_cache.clear()
        self.post_cache.clear()
        self.element_cache.clear()
        
    def get_cache_stats(self) -> Dict[str, Any]:
        """Статистика использования кэша"""
        total_requests = self.cache_stats['hits'] + self.cache_stats['misses']
        hit_ratio = self.cache_stats['hits'] / total_requests if total_requests > 0 else 0
        
        return {
            'hit_ratio': f"{hit_ratio:.2%}",
            'total_requests': total_requests,
            **self.cache_stats,
            'cache_sizes': {
                'url_cache': len(self.url_cache),
                'selector_cache': len(self.selector_cache),
                'post_cache': len(self.post_cache)
            }
        }

class ElementExtractor(ABC):
    """Абстрактный базовый класс для извлечения элементов"""
    
    def __init__(self, cache_manager: CacheManager, retry_manager: RetryManager, logger: LoggerManager):
        self.cache_manager = cache_manager
        self.retry_manager = retry_manager
        self.logger = logger
        
    @abstractmethod
    def extract(self, post_element) -> Any:
        """Абстрактный метод извлечения"""
        pass

class AuthorExtractor(ElementExtractor):
    """Извлечение информации об авторе"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.author_selectors = [
            'h2 a[role="link"]',
            'span.x193iq5w a',
            'h2.x1heor9g a',
            'div.x1heor9g a[role="link"]',
            'a.x1i10hfl[role="link"]:not([href*="groups"])',
            'span.xt0psk2 a',
            'div[role="article"] h2 a[role="link"]'
        ]
        
    @lru_cache(maxsize=500)
    def extract(self, post_element) -> Optional[AuthorInfo]:
        """Извлечение расширенной информации об авторе"""
        try:
            for selector in self.author_selectors:
                try:
                    element = post_element.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text:
                        # Базовая информация
                        name = element.text.strip()
                        profile_url = element.get_attribute('href')
                        if profile_url:
                            profile_url = profile_url.split('?')[0]
                        
                        # Дополнительная информация
                        avatar_url = self._extract_avatar(post_element)
                        is_verified = self._check_verification(post_element)
                        
                        return AuthorInfo(
                            name=name,
                            profile_url=profile_url,
                            avatar_url=avatar_url,
                            is_verified=is_verified
                        )
                except Exception as e:
                    self.logger.logger.debug(f"Failed selector {selector}: {e}")
                    continue
                    
            # Fallback: попробуем найти имя автора в заголовке поста
            try:
                header = post_element.find_element(By.CSS_SELECTOR, 'h2')
                if header and header.text:
                    return AuthorInfo(name=header.text.strip())
            except:
                pass
                
            return None
            
        except Exception as e:
            self.logger.log_error_with_context(e, {'method': 'extract_author'})
            return None
    
    def _extract_avatar(self, post_element) -> Optional[str]:
        """Извлечение URL аватара автора"""
        avatar_selectors = [
            'img[data-imgperflogname="profileCoverPhoto"]',
            'div[role="article"] img[src*="profile"]',
            'a[role="link"] img'
        ]
        
        for selector in avatar_selectors:
            try:
                img = post_element.find_element(By.CSS_SELECTOR, selector)
                src = img.get_attribute('src')
                if src and 'profile' in src:
                    return src
            except:
                continue
        return None
    
    def _check_verification(self, post_element) -> bool:
        """Проверка верификации пользователя"""
        verification_selectors = [
            'svg[aria-label*="Verified"]',
            '[data-testid="profile-verification-badge"]',
            'img[alt*="verified"]'
        ]
        
        for selector in verification_selectors:
            try:
                if post_element.find_element(By.CSS_SELECTOR, selector):
                    return True
            except:
                continue
        return False

class CommentExtractor(ElementExtractor):
    """Расширенное извлечение комментариев"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.comment_selectors = [
            'div[role="article"][aria-label*="Comment"]',
            'div.x1y332i5',
            'div[data-testid="UFI2Comment/root_depth_0"]',
            'div.x1lliihq.x6ikm8r.x10wlt62.x1n2onr6.xlyipyv.xuxw1ft'
        ]
        
    def extract(self, post_element) -> List[CommentInfo]:
        """Извлечение расширенных данных комментариев"""
        try:
            comments = []
            
            # Сначала пытаемся загрузить больше комментариев
            self._load_more_comments(post_element)
            
            for selector in self.comment_selectors:
                try:
                    comment_elements = post_element.find_elements(By.CSS_SELECTOR, selector)
                    self.logger.logger.debug(f"Found {len(comment_elements)} comments with selector: {selector}")
                    
                    for comment_element in comment_elements:
                        comment_data = self._extract_single_comment(comment_element)
                        if comment_data:
                            comments.append(comment_data)
                    
                    if comments:  # Если нашли комментарии, прекращаем поиск
                        break
                        
                except Exception as e:
                    self.logger.logger.debug(f"Error with comment selector {selector}: {e}")
                    continue
            
            self.logger.logger.info(f"Total comments extracted: {len(comments)}")
            return comments
            
        except Exception as e:
            self.logger.log_error_with_context(e, {'method': 'extract_comments'})
            return []
    
    def _load_more_comments(self, post_element):
        """Загружает больше комментариев нажатием на соответствующие кнопки"""
        view_more_selectors = [
            'span.x193iq5w.xeuugli.x13faqbe.x1vvkbs.x1xmvt09.x1lliihq.x1s928wv.xhkezso.x1gmr53x.x1cpjm7i.x1fgarty.x1943h6x.xudqn12.x3x7a5m.x6prxxf.xvq8zen.x1s688f.xi81zsa',
            'div[role="button"]:has-text("View more comments")',
            'div[role="button"]:has-text("View previous comments")',
            'span:contains("previous comments")',
            'span:contains("View more comments")'
        ]
        
        for selector in view_more_selectors:
            try:
                view_more_buttons = post_element.find_elements(By.CSS_SELECTOR, selector)
                for button in view_more_buttons:
                    try:
                        # Используем JavaScript для клика, чтобы избежать проблем с перекрытием
                        post_element.find_element(By.TAG_NAME, 'body').send_keys('')  # Получаем driver через post_element
                        driver = post_element._parent
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(2)  # Ждем загрузку комментариев
                    except Exception as e:
                        self.logger.logger.debug(f"Error clicking view more button: {e}")
                        continue
            except Exception as e:
                self.logger.logger.debug(f"Error with view more selector {selector}: {e}")
                continue
    
    def _extract_single_comment(self, comment_element) -> Optional[CommentInfo]:
        """Извлечение данных одного комментария с расширенными метаданными"""
        try:
            # Извлекаем автора комментария
            author_data = self._extract_comment_author(comment_element)
            if not author_data:
                return None
            
            # Извлекаем текст комментария
            comment_text = self._extract_comment_text(comment_element)
            if not comment_text:
                return None
            
            # Извлекаем время комментария
            comment_time = self._extract_comment_time(comment_element)
            
            # Извлекаем количество лайков
            likes_count = self._extract_comment_likes(comment_element)
            
            # Извлекаем количество ответов
            replies_count = self._extract_comment_replies_count(comment_element)
            
            # Проверяем, закреплен ли комментарий
            is_pinned = self._check_comment_pinned(comment_element)
            
            # Проверяем, отредактирован ли комментарий
            is_edited = self._check_comment_edited(comment_element)
            
            # Извлекаем реакции
            reactions = self._extract_comment_reactions(comment_element)
            
            # Извлекаем ответы на комментарий
            replies = self._extract_comment_replies(comment_element)
            
            return CommentInfo(
                author=author_data,
                text=comment_text,
                posted_time=comment_time or 'Unknown',
                scraped_time=datetime.utcnow().isoformat(),
                likes_count=likes_count,
                replies_count=replies_count,
                is_pinned=is_pinned,
                is_edited=is_edited,
                reactions=reactions,
                replies=replies
            )
            
        except Exception as e:
            self.logger.logger.debug(f"Error extracting single comment: {e}")
            return None
    
    def _extract_comment_author(self, comment_element) -> Optional[AuthorInfo]:
        """Извлечение автора комментария"""
        try:
            author_link = comment_element.find_element(By.CSS_SELECTOR, 'a[role="link"]')
            name = author_link.text.strip()
            profile_url = author_link.get_attribute('href')
            if profile_url:
                profile_url = profile_url.split('?')[0]
            
            return AuthorInfo(name=name, profile_url=profile_url)
        except:
            return None
    
    def _extract_comment_text(self, comment_element) -> Optional[str]:
        """Извлечение текста комментария"""
        text_selectors = [
            'div[data-ad-comet-preview="message"]',
            'div[dir="auto"]',
            'span[dir="auto"]'
        ]
        
        for selector in text_selectors:
            try:
                text_element = comment_element.find_element(By.CSS_SELECTOR, selector)
                text = text_element.text.strip()
                if text:
                    return text
            except:
                continue
        return None
    
    def _extract_comment_time(self, comment_element) -> Optional[str]:
        """Извлечение времени комментария"""
        try:
            time_element = comment_element.find_element(By.CSS_SELECTOR, 'a[role="link"] span')
            return time_element.get_attribute('title') or time_element.text
        except:
            return None
    
    def _extract_comment_likes(self, comment_element) -> int:
        """Извлечение количества лайков комментария"""
        like_selectors = [
            'span[aria-label*="like"]',
            'span[aria-label*="reaction"]',
            'div[aria-label*="like"]'
        ]
        
        for selector in like_selectors:
            try:
                like_element = comment_element.find_element(By.CSS_SELECTOR, selector)
                like_text = like_element.get_attribute('aria-label') or like_element.text
                # Попытка извлечь число из текста
                import re
                numbers = re.findall(r'\d+', like_text)
                if numbers:
                    return int(numbers[0])
            except:
                continue
        return 0
    
    def _extract_comment_replies_count(self, comment_element) -> int:
        """Извлечение количества ответов на комментарий"""
        reply_selectors = [
            'span:contains("repl")',
            'div[aria-label*="repl"]',
            'button:contains("repl")'
        ]
        
        for selector in reply_selectors:
            try:
                reply_element = comment_element.find_element(By.CSS_SELECTOR, selector)
                reply_text = reply_element.text
                import re
                numbers = re.findall(r'\d+', reply_text)
                if numbers:
                    return int(numbers[0])
            except:
                continue
        return 0
    
    def _check_comment_pinned(self, comment_element) -> bool:
        """Проверка, закреплен ли комментарий"""
        pin_selectors = [
            'svg[aria-label*="Pinned"]',
            'div[aria-label*="Pinned"]',
            'span:contains("Pinned")'
        ]
        
        for selector in pin_selectors:
            try:
                if comment_element.find_element(By.CSS_SELECTOR, selector):
                    return True
            except:
                continue
        return False
    
    def _check_comment_edited(self, comment_element) -> bool:
        """Проверка, отредактирован ли комментарий"""
        edit_selectors = [
            'span:contains("Edited")',
            'div[aria-label*="Edited"]',
            'span:contains("edited")'
        ]
        
        for selector in edit_selectors:
            try:
                if comment_element.find_element(By.CSS_SELECTOR, selector):
                    return True
            except:
                continue
        return False
    
    def _extract_comment_reactions(self, comment_element) -> Dict[str, int]:
        """Извлечение реакций на комментарий"""
        reactions = {}
        reaction_selectors = [
            'div[aria-label*="reaction"]',
            'span[data-testid*="reaction"]'
        ]
        
        for selector in reaction_selectors:
            try:
                reaction_elements = comment_element.find_elements(By.CSS_SELECTOR, selector)
                for element in reaction_elements:
                    aria_label = element.get_attribute('aria-label') or element.text
                    # Парсинг реакций из aria-label
                    # Например: "1 like, 2 love, 3 wow"
                    import re
                    reaction_matches = re.findall(r'(\d+)\s+(\w+)', aria_label.lower())
                    for count, reaction_type in reaction_matches:
                        reactions[reaction_type] = int(count)
            except:
                continue
        
        return reactions
    
    def _extract_comment_replies(self, comment_element) -> List[CommentInfo]:
        """Извлечение ответов на комментарий (рекурсивно)"""
        replies = []
        try:
            # Ищем вложенные комментарии
            reply_elements = comment_element.find_elements(
                By.CSS_SELECTOR, 
                'div[role="article"][aria-label*="Reply"]'
            )
            
            for reply_element in reply_elements[:5]:  # Ограничиваем количество ответов
                reply_data = self._extract_single_comment(reply_element)
                if reply_data:
                    replies.append(reply_data)
                    
        except Exception as e:
            self.logger.logger.debug(f"Error extracting comment replies: {e}")
        
        return replies

class PostProcessor:
    """Асинхронная обработка постов"""
    
    def __init__(self, config: ScrapingConfig, logger: LoggerManager, 
                 cache_manager: CacheManager, retry_manager: RetryManager):
        self.config = config
        self.logger = logger
        self.cache_manager = cache_manager
        self.retry_manager = retry_manager
        self.memory_manager = MemoryManager(logger)
        
        # Инициализируем экстракторы
        self.author_extractor = AuthorExtractor(cache_manager, retry_manager, logger)
        self.comment_extractor = CommentExtractor(cache_manager, retry_manager, logger)
        
        # Очередь для обработки постов
        self.processing_queue = Queue(maxsize=config.max_posts * 2)
        self.results_queue = Queue()
        
        # Флаг для остановки обработки
        self.stop_processing = threading.Event()
        
    def start_async_processing(self):
        """Запуск асинхронной обработки постов"""
        self.workers = []
        
        for i in range(self.config.parallel_workers):
            worker = threading.Thread(
                target=self._process_worker,
                name=f"PostProcessor-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
            
        self.logger.logger.info(f"Started {len(self.workers)} processing workers")
    
    def _process_worker(self):
        """Рабочий поток для обработки постов"""
        while not self.stop_processing.is_set():
            try:
                # Получаем пост из очереди с таймаутом
                post_element = self.processing_queue.get(timeout=1.0)
                
                with self.memory_manager.memory_monitoring(f"process_post"):
                    result = self._process_single_post(post_element)
                    if result:
                        self.results_queue.put(result)
                
                self.processing_queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                self.logger.log_error_with_context(e, {'worker': threading.current_thread().name})
    
    def add_post_for_processing(self, post_element):
        """Добавление поста в очередь обработки"""
        try:
            self.processing_queue.put(post_element, timeout=1.0)
        except Full:
            self.logger.logger.warning("Processing queue is full, skipping post")
    
    def get_processed_posts(self) -> List[PostInfo]:
        """Получение обработанных постов"""
        results = []
        while not self.results_queue.empty():
            try:
                result = self.results_queue.get_nowait()
                results.append(result)
            except Empty:
                break
        return results
    
    def stop_async_processing(self):
        """Остановка асинхронной обработки"""
        self.stop_processing.set()
        
        # Ждем завершения всех воркеров
        for worker in self.workers:
            worker.join(timeout=5.0)
            
        self.logger.logger.info("Stopped all processing workers")
    
    @retry_manager.retry
    def _process_single_post(self, post_element) -> Optional[PostInfo]:
        """Обработка одного поста с повторными попытками"""
        try:
            start_time = time.time()
            
            # Проверяем кэш
            post_url = self._get_post_url(post_element)
            if not post_url:
                return None
                
            cached_post = self.cache_manager.get_cached_post(post_url)
            if cached_post:
                self.logger.logger.debug(f"Using cached data for post: {post_url}")
                return cached_post
            
            # Извлекаем автора поста
            author_data = self.author_extractor.extract(post_element)
            if not author_data:
                self.logger.logger.warning(f"Could not extract author for post: {post_url}")
                return None
            
            # Извлекаем содержимое поста
            post_content = self._extract_post_content(post_element)
            
            # Извлекаем время поста
            post_time = self._extract_post_time(post_element)
            
            # Извлекаем внешние ссылки
            external_links = self._extract_external_links(post_element)
            
            # Извлекаем изображения
            images = self._extract_images(post_element)
            
            # Извлекаем метаданные реакций и взаимодействий
            likes_count = self._extract_likes_count(post_element)
            shares_count = self._extract_shares_count(post_element)
            reactions = self._extract_reactions(post_element)
            post_type = self._detect_post_type(post_element)
            tags = self._extract_hashtags(post_content)
            
            # Извлекаем комментарии (асинхронно если включено)
            comments = []
            if self.config.enable_async:
                comments = await self._extract_comments_async(post_element)
            else:
                comments = self.comment_extractor.extract(post_element)
            
            # Создаем объект поста
            post_data = PostInfo(
                author=author_data,
                content=post_content or "",
                posted_time=post_time or 'Unknown',
                post_url=post_url,
                external_links=external_links,
                images=images,
                comments=comments,
                scraped_time=datetime.utcnow().isoformat(),
                likes_count=likes_count,
                shares_count=shares_count,
                reactions=reactions,
                post_type=post_type,
                tags=tags
            )
            
            # Кэшируем данные поста
            self.cache_manager.cache_post_data(post_url, post_data)
            
            # Логируем производительность
            processing_time = time.time() - start_time
            self.logger.log_performance("process_single_post", processing_time, post_url=post_url)
            
            return post_data
            
        except Exception as e:
            self.logger.log_error_with_context(e, {
                'method': '_process_single_post',
                'post_url': getattr(self, '_current_post_url', 'unknown')
            })
            return None
    
    def _get_post_url(self, post_element) -> Optional[str]:
        """Извлечение URL поста"""
        url_selectors = [
            'a[href*="/posts/"]',
            'a[href*="/photos/"]',
            'a[href*="/videos/"]',
            'a[aria-label*="minutes"][href]',
            'a[aria-label*="hours"][href]',
            'a[aria-label*="days"][href]',
            'span._6n3u a[href]',
            'div[data-testid="story-subtitle"] a'
        ]
        
        for selector in url_selectors:
            try:
                link_element = post_element.find_element(By.CSS_SELECTOR, selector)
                href = link_element.get_attribute('href')
                if href and ('posts' in href or 'photos' in href or 'videos' in href):
                    # Очищаем URL от лишних параметров
                    clean_url = href.split('?')[0] if '?' in href else href
                    return clean_url
            except:
                continue
        
        # Fallback: пытаемся найти любую ссылку с временной меткой
        try:
            timestamp_links = post_element.find_elements(By.CSS_SELECTOR, 'a[href*="facebook.com"]')
            for link in timestamp_links:
                href = link.get_attribute('href')
                if href and any(x in href for x in ['/posts/', '/photos/', '/videos/']):
                    return href.split('?')[0]
        except:
            pass
            
        return None
    
    def _extract_post_content(self, post_element) -> Optional[str]:
        """Извлечение содержимого поста с улучшенными селекторами"""
        content_selectors = [
            'div[data-ad-comet-preview="message"] span',
            'div[data-testid="post_message"]',
            'div.x11i5rnm.xat24cr.x1mh8g0r.x1vvkbs span[dir="auto"]',
            'div[class*="userContent"] span',
            'div.userContent p',
            'span.x193iq5w.xeuugli.x13faqbe.x1vvkbs.x1xmvt09.x1lliihq.x1s928wv.xhkezso.x1gmr53x.x1cpjm7i.x1fgarty.x1943h6x.x4zkp8e.x676frb.x1nxh6w3.x1sibtaa.x1s688f.xzsf02u',
            'div[role="article"] span[dir="auto"]:not([aria-hidden="true"])'
        ]
        
        for selector in content_selectors:
            try:
                content_elements = post_element.find_elements(By.CSS_SELECTOR, selector)
                if content_elements:
                    # Объединяем текст из всех найденных элементов
                    content_parts = []
                    for element in content_elements:
                        text = element.text.strip()
                        if text and len(text) > 10:  # Игнорируем слишком короткие фрагменты
                            content_parts.append(text)
                    
                    if content_parts:
                        return ' '.join(content_parts)
            except Exception as e:
                self.logger.logger.debug(f"Error with content selector {selector}: {e}")
                continue
        
        return None
    
    def _extract_post_time(self, post_element) -> Optional[str]:
        """Извлечение времени публикации поста"""
        time_selectors = [
            'abbr[data-utime]',
            'a[aria-label*="minutes"]',
            'a[aria-label*="hours"]',
            'a[aria-label*="days"]',
            'span[title]',
            'abbr[title]',
            'a[href*="posts"] span[title]'
        ]
        
        for selector in time_selectors:
            try:
                time_element = post_element.find_element(By.CSS_SELECTOR, selector)
                
                # Пытаемся получить точное время из атрибутов
                if time_element.tag_name == 'abbr':
                    utime = time_element.get_attribute('data-utime')
                    if utime:
                        timestamp = int(utime)
                        return datetime.fromtimestamp(timestamp).isoformat()
                
                # Пытаемся получить из title атрибута
                title = time_element.get_attribute('title')
                if title:
                    return title
                
                # Пытаемся получить из aria-label
                aria_label = time_element.get_attribute('aria-label')
                if aria_label:
                    return aria_label
                
                # Получаем текст элемента
                text = time_element.text.strip()
                if text:
                    return text
                    
            except Exception as e:
                self.logger.logger.debug(f"Error with time selector {selector}: {e}")
                continue
        
        return None
    
    def _extract_external_links(self, post_element) -> List[str]:
        """Извлечение внешних ссылок из поста"""
        external_links = []
        
        try:
            # Ищем все ссылки в посте
            link_elements = post_element.find_elements(By.CSS_SELECTOR, 'a[href]')
            
            for link in link_elements:
                href = link.get_attribute('href')
                if href:
                    # Фильтруем только внешние ссылки (не Facebook)
                    if not any(fb_domain in href for fb_domain in ['facebook.com', 'fb.com', 'instagram.com']):
                        # Очищаем ссылку от Facebook-редиректов
                        if 'facebook.com/l.php' in href:
                            # Извлекаем оригинальную ссылку из параметра u
                            import urllib.parse
                            parsed = urllib.parse.urlparse(href)
                            params = urllib.parse.parse_qs(parsed.query)
                            if 'u' in params:
                                original_url = urllib.parse.unquote(params['u'][0])
                                external_links.append(original_url)
                        else:
                            external_links.append(href)
        
        except Exception as e:
            self.logger.logger.debug(f"Error extracting external links: {e}")
        
        return list(set(external_links))  # Удаляем дубликаты
    
    def _extract_images(self, post_element) -> List[str]:
        """Извлечение изображений из поста"""
        images = []
        
        image_selectors = [
            'img[src*="scontent"]',
            'img[data-src*="scontent"]',
            'div[role="img"] img',
            'div[data-testid="photo"] img',
            'img[alt]:not([alt=""])'
        ]
        
        for selector in image_selectors:
            try:
                img_elements = post_element.find_elements(By.CSS_SELECTOR, selector)
                for img in img_elements:
                    src = img.get_attribute('src') or img.get_attribute('data-src')
                    if src and 'scontent' in src:  # Facebook CDN изображения
                        # Получаем оригинальное разрешение
                        if '&_nc_cat=' in src:
                            # Убираем параметры масштабирования для получения оригинала
                            clean_src = re.sub(r'&s=\d+x\d+', '', src)
                            images.append(clean_src)
                        else:
                            images.append(src)
            except Exception as e:
                self.logger.logger.debug(f"Error with image selector {selector}: {e}")
                continue
        
        return list(set(images))  # Удаляем дубликаты
    
    def _extract_likes_count(self, post_element) -> int:
        """Извлечение количества лайков"""
        like_selectors = [
            'span[aria-label*="reaction"]',
            'div[aria-label*="reaction"]',
            'span[data-testid="UFI2ReactionsCount/root"]',
            'div._81hb span',
            'span._3dlh._3dli'
        ]
        
        for selector in like_selectors:
            try:
                like_element = post_element.find_element(By.CSS_SELECTOR, selector)
                aria_label = like_element.get_attribute('aria-label') or like_element.text
                
                # Извлекаем числа из текста
                numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?)', aria_label.replace(',', ''))
                if numbers:
                    # Преобразуем в число (обрабатываем K, M)
                    count_str = numbers[0]
                    if 'K' in aria_label.upper():
                        return int(float(count_str) * 1000)
                    elif 'M' in aria_label.upper():
                        return int(float(count_str) * 1000000)
                    else:
                        return int(count_str)
                        
            except Exception as e:
                self.logger.logger.debug(f"Error with like selector {selector}: {e}")
                continue
        
        return 0
    
    def _extract_shares_count(self, post_element) -> int:
        """Извлечение количества репостов"""
        share_selectors = [
            'span[aria-label*="share"]',
            'div[aria-label*="share"]',
            'span[data-testid*="share"]',
            'div._3dlh._3dli:contains("share")'
        ]
        
        for selector in share_selectors:
            try:
                share_element = post_element.find_element(By.CSS_SELECTOR, selector)
                aria_label = share_element.get_attribute('aria-label') or share_element.text
                
                numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?)', aria_label.replace(',', ''))
                if numbers:
                    count_str = numbers[0]
                    if 'K' in aria_label.upper():
                        return int(float(count_str) * 1000)
                    elif 'M' in aria_label.upper():
                        return int(float(count_str) * 1000000)
                    else:
                        return int(count_str)
                        
            except Exception as e:
                self.logger.logger.debug(f"Error with share selector {selector}: {e}")
                continue
        
        return 0
    
    def _extract_reactions(self, post_element) -> Dict[str, int]:
        """Извлечение детализированных реакций"""
        reactions = {}
        
        # Пытаемся найти детальную информацию о реакциях
        reaction_selectors = [
            'div[aria-label*="reaction"]',
            'span[data-testid="UFI2ReactionsCount/root"]',
            'div._1g06 span'
        ]
        
        for selector in reaction_selectors:
            try:
                reaction_element = post_element.find_element(By.CSS_SELECTOR, selector)
                aria_label = reaction_element.get_attribute('aria-label')
                
                if aria_label:
                    # Парсим различные типы реакций
                    reaction_patterns = {
                        'like': r'(\d+)\s*(?:people\s*)?(?:reacted\s*with\s*)?(?:liked|like)',
                        'love': r'(\d+)\s*(?:people\s*)?(?:reacted\s*with\s*)?love',
                        'haha': r'(\d+)\s*(?:people\s*)?(?:reacted\s*with\s*)?(?:haha|laugh)',
                        'wow': r'(\d+)\s*(?:people\s*)?(?:reacted\s*with\s*)?wow',
                        'sad': r'(\d+)\s*(?:people\s*)?(?:reacted\s*with\s*)?(?:sad|cry)',
                        'angry': r'(\d+)\s*(?:people\s*)?(?:reacted\s*with\s*)?angry'
                    }
                    
                    for reaction_type, pattern in reaction_patterns.items():
                        matches = re.findall(pattern, aria_label.lower())
                        if matches:
                            reactions[reaction_type] = int(matches[0])
                    
                    # Если не смогли распарсить детально, берем общее количество
                    if not reactions:
                        total_match = re.search(r'(\d+)', aria_label)
                        if total_match:
                            reactions['total'] = int(total_match.group(1))
                    
                    break
                    
            except Exception as e:
                self.logger.logger.debug(f"Error with reaction selector {selector}: {e}")
                continue
        
        return reactions
    
    def _detect_post_type(self, post_element) -> str:
        """Определение типа поста"""
        try:
            # Проверяем наличие видео
            if post_element.find_elements(By.CSS_SELECTOR, 'video, div[aria-label*="video"]'):
                return "video"
            
            # Проверяем наличие изображений
            if post_element.find_elements(By.CSS_SELECTOR, 'img[src*="scontent"]'):
                return "photo"
            
            # Проверяем наличие ссылок на внешние ресурсы
            external_links = self._extract_external_links(post_element)
            if external_links:
                return "link"
            
            # Проверяем опросы
            if post_element.find_elements(By.CSS_SELECTOR, 'div[aria-label*="poll"], div[data-testid*="poll"]'):
                return "poll"
            
            # Проверяем события
            if post_element.find_elements(By.CSS_SELECTOR, 'div[aria-label*="event"]'):
                return "event"
            
            # По умолчанию - текстовый пост
            return "text"
            
        except Exception as e:
            self.logger.logger.debug(f"Error detecting post type: {e}")
            return "unknown"
    
    def _extract_hashtags(self, content: str) -> List[str]:
        """Извлечение хэштегов из содержимого"""
        if not content:
            return []
        
        # Регулярное выражение для поиска хэштегов
        hashtag_pattern = r'#[A-Za-z0-9_А-Яа-я]+'
        hashtags = re.findall(hashtag_pattern, content)
        
        return list(set(hashtags))  # Удаляем дубликаты
    
    async def _extract_comments_async(self, post_element) -> List[CommentInfo]:
        """Асинхронное извлечение комментариев"""
        try:
            loop = asyncio.get_event_loop()
            
            # Запускаем извлечение комментариев в отдельном потоке
            comments = await loop.run_in_executor(
                None, 
                self.comment_extractor.extract, 
                post_element
            )
            
            return comments
            
        except Exception as e:
            self.logger.logger.debug(f"Error in async comment extraction: {e}")
            # Fallback к синхронному методу
            return self.comment_extractor.extract(post_element)

class PerformanceMonitor:
    """Монитор производительности и ресурсов"""
    
    def __init__(self, config: ScrapingConfig, logger: LoggerManager):
        self.config = config
        self.logger = logger
        self.start_time = time.time()
        self.metrics = {
            'posts_processed': 0,
            'comments_extracted': 0,
            'errors_count': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'memory_peaks': [],
            'processing_times': []
        }
        
        # Флаг для мониторинга
        self.monitoring_active = True
        self.monitor_thread = None
        
    def start_monitoring(self):
        """Запуск мониторинга производительности"""
        self.monitor_thread = threading.Thread(
            target=self._monitor_resources,
            daemon=True,
            name="PerformanceMonitor"
        )
        self.monitor_thread.start()
        self.logger.logger.info("Performance monitoring started")
    
    def _monitor_resources(self):
        """Мониторинг ресурсов системы"""
        while self.monitoring_active:
            try:
                # Мониторинг памяти
                memory_percent = psutil.virtual_memory().percent
                self.metrics['memory_peaks'].append(memory_percent)
                
                # Предупреждение при высоком использовании памяти
                if memory_percent > self.config.max_memory_usage:
                    self.logger.logger.warning(
                        f"High memory usage: {memory_percent:.1f}% (threshold: {self.config.max_memory_usage}%)"
                    )
                    # Принудительная очистка
                    gc.collect()
                
                # Мониторинг GPU (если включен)
                if self.config.enable_gpu:
                    try:
                        gpus = GPUtil.getGPUs()
                        if gpus:
                            gpu = gpus[0]
                            if gpu.memoryUtil > 0.8:  # 80% использования GPU памяти
                                self.logger.logger.warning(
                                    f"High GPU memory usage: {gpu.memoryUtil:.1%}"
                                )
                    except:
                        pass
                
                # Очистка старых метрик
                if len(self.metrics['memory_peaks']) > 100:
                    self.metrics['memory_peaks'] = self.metrics['memory_peaks'][-50:]
                
                if len(self.metrics['processing_times']) > 100:
                    self.metrics['processing_times'] = self.metrics['processing_times'][-50:]
                
                time.sleep(5)  # Проверка каждые 5 секунд
                
            except Exception as e:
                self.logger.logger.debug(f"Error in resource monitoring: {e}")
                time.sleep(10)
    
    def record_post_processed(self, processing_time: float):
        """Запись обработанного поста"""
        self.metrics['posts_processed'] += 1
        self.metrics['processing_times'].append(processing_time)
    
    def record_comments_extracted(self, count: int):
        """Запись извлеченных комментариев"""
        self.metrics['comments_extracted'] += count
    
    def record_error(self):
        """Запись ошибки"""
        self.metrics['errors_count'] += 1
    
    def record_cache_hit(self):
        """Запись попадания в кэш"""
        self.metrics['cache_hits'] += 1
    
    def record_cache_miss(self):
        """Запись промаха кэша"""
        self.metrics['cache_misses'] += 1
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Получение отчета о производительности"""
        current_time = time.time()
        total_time = current_time - self.start_time
        
        avg_processing_time = (
            sum(self.metrics['processing_times']) / len(self.metrics['processing_times'])
            if self.metrics['processing_times'] else 0
        )
        
        posts_per_minute = (
            self.metrics['posts_processed'] / (total_time / 60)
            if total_time > 0 else 0
        )
        
        cache_hit_rate = (
            self.metrics['cache_hits'] / (self.metrics['cache_hits'] + self.metrics['cache_misses'])
            if (self.metrics['cache_hits'] + self.metrics['cache_misses']) > 0 else 0
        )
        
        current_memory = psutil.virtual_memory().percent
        max_memory = max(self.metrics['memory_peaks']) if self.metrics['memory_peaks'] else current_memory
        
        return {
            'total_runtime': f"{total_time:.2f}s",
            'posts_processed': self.metrics['posts_processed'],
            'comments_extracted': self.metrics['comments_extracted'],
            'errors_count': self.metrics['errors_count'],
            'posts_per_minute': f"{posts_per_minute:.2f}",
            'avg_processing_time': f"{avg_processing_time:.2f}s",
            'cache_hit_rate': f"{cache_hit_rate:.2%}",
            'current_memory_usage': f"{current_memory:.1f}%",
            'peak_memory_usage': f"{max_memory:.1f}%",
            'memory_threshold': f"{self.config.max_memory_usage:.1f}%"
        }
    
    def stop_monitoring(self):
        """Остановка мониторинга"""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        self.logger.logger.info("Performance monitoring stopped")

class CheckpointManager:
    """Управление чекпоинтами для восстановления после сбоев"""
    
    def __init__(self, config: ScrapingConfig, logger: LoggerManager):
        self.config = config
        self.logger = logger
        self.checkpoint_dir = Path(config.output_dir) / "checkpoints"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        self.current_checkpoint = {
            'processed_posts': [],
            'last_scroll_position': 0,
            'timestamp': datetime.utcnow().isoformat(),
            'config': asdict(config)
        }
    
    def save_checkpoint(self, processed_posts: List[PostInfo], scroll_position: int):
        """Сохранение чекпоинта"""
        try:
            self.current_checkpoint.update({
                'processed_posts': [asdict(post) for post in processed_posts],
                'last_scroll_position': scroll_position,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            checkpoint_file = self.checkpoint_dir / f"checkpoint_{int(time.time())}.json"
            
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(self.current_checkpoint, f, ensure_ascii=False, indent=2)
            
            # Удаляем старые чекпоинты (оставляем последние 5)
            self._cleanup_old_checkpoints()
            
            self.logger.logger.info(f"Checkpoint saved: {checkpoint_file}")
            
        except Exception as e:
            self.logger.log_error_with_context(e, {'method': 'save_checkpoint'})
    
    def load_latest_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Загрузка последнего чекпоинта"""
        try:
            checkpoint_files = list(self.checkpoint_dir.glob("checkpoint_*.json"))
            if not checkpoint_files:
                return None
            
            # Находим самый свежий чекпоинт
            latest_checkpoint = max(checkpoint_files, key=lambda f: f.stat().st_mtime)
            
            with open(latest_checkpoint, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
            
            self.logger.logger.info(f"Loaded checkpoint: {latest_checkpoint}")
            return checkpoint_data
            
        except Exception as e:
            self.logger.log_error_with_context(e, {'method': 'load_latest_checkpoint'})
            return None
    
    def _cleanup_old_checkpoints(self):
        """Очистка старых чекпоинтов"""
        try:
            checkpoint_files = sorted(
                self.checkpoint_dir.glob("checkpoint_*.json"),
                key=lambda f: f.stat().st_mtime,
                reverse=True
            )
            
            # Удаляем все кроме последних 5
            for old_checkpoint in checkpoint_files[5:]:
                old_checkpoint.unlink()
                
        except Exception as e:
            self.logger.logger.debug(f"Error cleaning old checkpoints: {e}")

class EnhancedFacebookScraper:
    """Улучшенный скрапер Facebook групп с расширенными возможностями"""
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        
        # Инициализация менеджеров
        self.logger = LoggerManager(config)
        self.retry_manager = RetryManager(
            max_attempts=config.retry_attempts,
            delay=config.retry_delay
        )
        self.cache_manager = CacheManager(config, self.logger)
        self.memory_manager = MemoryManager(self.logger)
        self.performance_monitor = PerformanceMonitor(config, self.logger)
        self.checkpoint_manager = CheckpointManager(config, self.logger)
        
        # Обработчик постов
        self.post_processor = PostProcessor(
            config, self.logger, self.cache_manager, self.retry_manager
        )
        
        # Веб-драйвер
        self.driver = None
        self.scraped_posts = []
        
        # Обработчик сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.logger.info("Enhanced Facebook Scraper initialized")
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для корректного завершения"""
        self.logger.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self._cleanup()
        sys.exit(0)
    
    def _cleanup(self):
        """Очистка ресурсов"""
        try:
            # Сохраняем чекпоинт
            if self.scraped_posts:
                self.checkpoint_manager.save_checkpoint(
                    self.scraped_posts, 
                    self.driver.execute_script("return window.pageYOffset;") if self.driver else 0
                )
            
            # Останавливаем мониторинг
            self.performance_monitor.stop_monitoring()
            
            # Останавливаем обработку постов
            if hasattr(self.post_processor, 'stop_async_processing'):
                self.post_processor.stop_async_processing()
            
            # Закрываем драйвер
            if self.driver:
                self.driver.quit()
                
            # Очищаем кэш
            self.cache_manager.clear_cache()
            
            self.logger.logger.info("Resources cleaned up successfully.")
            
        except Exception as e:
            self.logger.log_error_with_context(e, {'method': '_cleanup'})

    def _initialize_driver(self):
        """Инициализация undetected_chromedriver"""
        try:
            self.logger.logger.info("Initializing undetected_chromedriver...")
            
            # Настройка опций Chrome
            options = uc.ChromeOptions()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu') # Отключаем GPU, так как uc иногда конфликтует
            options.add_argument('--start-maximized')
            options.add_argument('--disable-notifications')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-blink-features=AutomationControlled') # Для обхода детекторов
            
            # Дополнительные опции для скрытия автоматизации
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)

            # Проверяем, есть ли GPU и включено ли его использование
            if self.config.enable_gpu and GPUtil.getGPUs():
                options.add_argument('--enable-gpu')
                self.logger.logger.info("GPU usage enabled.")
            else:
                self.logger.logger.info("GPU usage disabled or not available.")

            # Инициализация драйвера
            self.driver = uc.Chrome(options=options)
            self.driver.set_page_load_timeout(self.config.page_load_timeout)
            self.driver.implicitly_wait(self.config.implicit_wait)
            
            self.logger.logger.info("Chromedriver initialized successfully.")
            
        except Exception as e:
            self.logger.log_error_with_context(e, {'method': '_initialize_driver'})
            raise

    def _load_cookies(self):
        """Загрузка и применение куки"""
        if not os.path.exists(self.config.cookies_file):
            self.logger.logger.warning(f"Cookies file not found: {self.config.cookies_file}")
            return False
            
        try:
            with open(self.config.cookies_file, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            
            # Необходимо перейти на домен, прежде чем добавлять куки
            self.driver.get("https://www.facebook.com")
            
            for cookie in cookies:
                # Undetected Chromedriver может иметь проблемы с некоторыми полями куки.
                # Удаляем 'SameSite' если он присутствует и вызывает ошибку
                if 'SameSite' in cookie:
                    del cookie['SameSite']
                
                # Некоторые поля могут быть невалидными, обрабатываем ошибки
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    self.logger.logger.warning(f"Failed to add cookie {cookie.get('name')}: {e}")
                    
            self.driver.refresh() # Обновляем страницу для применения куки
            self.logger.logger.info("Cookies loaded and applied.")
            return True
            
        except Exception as e:
            self.logger.log_error_with_context(e, {'method': '_load_cookies'})
            return False

    def _save_cookies(self):
        """Сохранение текущих куки в файл"""
        try:
            cookies = self.driver.get_cookies()
            with open(self.config.cookies_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
            self.logger.logger.info("Cookies saved.")
        except Exception as e:
            self.logger.log_error_with_context(e, {'method': '_save_cookies'})

    def _navigate_to_group(self):
        """Переход на страницу группы"""
        try:
            self.logger.logger.info(f"Navigating to group URL: {self.config.group_url}")
            self.driver.get(self.config.group_url)
            
            # Ожидание загрузки страницы
            WebDriverWait(self.driver, self.config.page_load_timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
            )
            self.logger.logger.info("Successfully navigated to group page.")
            return True
            
        except TimeoutException:
            self.logger.log_error_with_context(
                TimeoutException("Page load timeout or feed element not found."),
                {'url': self.config.group_url, 'method': '_navigate_to_group'}
            )
            return False
        except Exception as e:
            self.logger.log_error_with_context(e, {'url': self.config.group_url, 'method': '_navigate_to_group'})
            return False

    def _scroll_down(self, scroll_attempts: int):
        """Прокрутка страницы вниз для загрузки новых постов"""
        current_scroll_attempts = 0
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        
        while current_scroll_attempts < scroll_attempts and len(self.scraped_posts) < self.config.max_posts:
            self.logger.logger.info(
                f"Scrolling down... Attempt {current_scroll_attempts + 1}/{scroll_attempts}, "
                f"Posts scraped: {len(self.scraped_posts)}/{self.config.max_posts}"
            )
            
            # Прокрутка до конца страницы
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(self.config.scroll_delay)
            
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                self.logger.logger.info("End of page reached or no new content loaded.")
                # Попробуем нажать на кнопку "Показать больше" если она есть
                if not self._click_load_more_button():
                    break # Если нет новых данных и кнопка не найдена, выходим
            
            last_height = new_height
            current_scroll_attempts += 1
            
            # Дополнительная задержка после прокрутки
            time.sleep(1) 
            
            # Сохранение чекпоинта каждые N прокруток
            if current_scroll_attempts % 10 == 0:
                self.checkpoint_manager.save_checkpoint(
                    self.scraped_posts, 
                    self.driver.execute_script("return window.pageYOffset;")
                )
        
        self.logger.logger.info(f"Finished scrolling. Total scroll attempts: {current_scroll_attempts}")

    def _click_load_more_button(self) -> bool:
        """Попытка нажать на кнопку 'Показать больше'"""
        load_more_selectors = [
            'div[role="button"]:contains("See more")',
            'span:contains("See more posts")',
            'div[role="button"][tabindex="0"]',
            'a[aria-label*="See more"]'
        ]
        
        for selector in load_more_selectors:
            try:
                button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                self.driver.execute_script("arguments[0].click();", button)
                self.logger.logger.info(f"Clicked 'Load More' button with selector: {selector}")
                time.sleep(self.config.scroll_delay) # Ждем загрузки нового контента
                return True
            except TimeoutException:
                continue # Кнопка не найдена по этому селектору
            except Exception as e:
                self.logger.logger.debug(f"Error clicking load more button with selector {selector}: {e}")
                continue
        self.logger.logger.debug("No 'Load More' button found or clickable.")
        return False

    def _get_post_elements(self) -> List[Any]:
        """Получение элементов постов со страницы"""
        post_selectors = [
            'div[role="article"]',
            'div[data-pagelet="FeedUnit_"]',
            'div.x1yztbdb.x1n2onr6.xh8yej3.x1ja2u2z' # Новый селектор для постов
        ]
        
        post_elements = []
        for selector in post_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    self.logger.logger.debug(f"Found {len(elements)} post elements with selector: {selector}")
                    post_elements.extend(elements)
            except Exception as e:
                self.logger.logger.debug(f"Error finding post elements with selector {selector}: {e}")
                continue
        
        # Удаляем дубликаты элементов (если один и тот же пост попадает под разные селекторы)
        unique_post_elements = []
        seen_ids = set()
        for element in post_elements:
            try:
                # Попытка получить уникальный идентификатор поста, если доступен
                post_id = element.get_attribute('data-story-id') or element.id
                if post_id and post_id not in seen_ids:
                    unique_post_elements.append(element)
                    seen_ids.add(post_id)
                elif not post_id: # Если нет ID, просто добавляем (риск дубликатов)
                    unique_post_elements.append(element)
            except Exception as e:
                self.logger.logger.debug(f"Could not get ID for element: {e}")
                unique_post_elements.append(element)

        self.logger.logger.info(f"Total unique post elements found: {len(unique_post_elements)}")
        return unique_post_elements

    def scrape(self) -> List[PostInfo]:
        """Основной метод скрапинга"""
        self.performance_monitor.start_monitoring()
        self.post_processor.start_async_processing()
        
        try:
            self._initialize_driver()
            
            # Попытка загрузить последний чекпоинт
            last_checkpoint = self.checkpoint_manager.load_latest_checkpoint()
            if last_checkpoint:
                self.scraped_posts = [PostInfo(**p) for p in last_checkpoint['processed_posts']]
                self.logger.logger.info(
                    f"Resuming from checkpoint with {len(self.scraped_posts)} "
                    f"posts and scroll position {last_checkpoint['last_scroll_position']}"
                )
                # Переходим на страницу и прокручиваем до последней позиции
                self._navigate_to_group()
                self.driver.execute_script(f"window.scrollTo(0, {last_checkpoint['last_scroll_position']});")
                time.sleep(self.config.scroll_delay) # Даем время на загрузку
            else:
                if not self._load_cookies() or not self._navigate_to_group():
                    self.logger.logger.critical("Initial setup (cookies or navigation) failed. Aborting.")
                    return []
                
            retrieved_posts_count = 0
            scroll_attempts = 0

            while retrieved_posts_count < self.config.max_posts and scroll_attempts < self.config.max_scroll_attempts:
                start_scrape_cycle_time = time.time()
                
                # Получаем все текущие элементы постов
                post_elements = self._get_post_elements()
                
                # Отправляем необработанные посты в асинхронный обработчик
                for element in post_elements:
                    # Проверяем, был ли этот пост уже обработан (по URL, если возможно)
                    post_url = self.post_processor._get_post_url(element)
                    if post_url and post_url not in [p.post_url for p in self.scraped_posts]:
                        self.post_processor.add_post_for_processing(element)
                        
                # Получаем обработанные посты из очереди результатов
                newly_processed_posts = self.post_processor.get_processed_posts()
                for post in newly_processed_posts:
                    if post.post_url not in [p.post_url for p in self.scraped_posts]:
                        self.scraped_posts.append(post)
                        self.performance_monitor.record_post_processed(
                            time.time() - start_scrape_cycle_time # Приблизительное время обработки
                        )
                        self.performance_monitor.record_comments_extracted(len(post.comments))
                        retrieved_posts_count = len(self.scraped_posts)
                        
                        if retrieved_posts_count >= self.config.max_posts:
                            self.logger.logger.info(
                                f"Reached max_posts ({self.config.max_posts}). Stopping scraping."
                            )
                            break

                self.logger.logger.info(
                    f"Scraped {retrieved_posts_count} posts so far. "
                    f"Queue size: {self.post_processor.processing_queue.qsize()}"
                )
                
                # Прокрутка страницы для загрузки новых постов
                if retrieved_posts_count < self.config.max_posts:
                    self._scroll_down(1) # Прокручиваем по одному разу за цикл
                    scroll_attempts += 1
                
                # Сохранение прогресса
                if retrieved_posts_count > 0 and retrieved_posts_count % self.config.batch_size == 0:
                     self.checkpoint_manager.save_checkpoint(
                        self.scraped_posts, 
                        self.driver.execute_script("return window.pageYOffset;")
                    )
                
                # Условие выхода, если новые посты не появляются
                if len(newly_processed_posts) == 0 and scroll_attempts > 5: # Если 5 прокруток ничего не дали
                    self.logger.logger.warning("No new posts found after several scrolls. Exiting loop.")
                    break
                    
                time.sleep(self.config.scroll_delay) # Пауза между итерациями скрапинга

            self.logger.logger.info("Scraping finished. Waiting for remaining posts to be processed...")
            self.post_processor.processing_queue.join() # Ждем завершения всех задач в очереди

            # Забираем последние обработанные посты
            final_processed_posts = self.post_processor.get_processed_posts()
            for post in final_processed_posts:
                if post.post_url not in [p.post_url for p in self.scraped_posts]:
                    self.scraped_posts.append(post)

            self._save_cookies() # Сохраняем куки после успешного скрапинга
            self.logger.logger.info("All posts processed and scraping completed.")
            
        except Exception as e:
            self.logger.log_error_with_context(e, {'method': 'scrape'})
        finally:
            self._cleanup()
            self.logger.logger.info("Scraping process finished. Performance report:")
            self.logger.logger.info(json.dumps(self.performance_monitor.get_performance_report(), indent=2))
            self.logger.logger.info(json.dumps(self.cache_manager.get_cache_stats(), indent=2))
        
        return self.scraped_posts

    def save_results(self, posts: List[PostInfo], filename: str = "scraped_posts.json"):
        """Сохранение результатов скрапинга в JSON файл"""
        output_path = os.path.join(self.config.output_dir, filename)
        
        try:
            # Преобразуем объекты PostInfo в словари для JSON сериализации
            serializable_posts = [asdict(post) for post in posts]
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(serializable_posts, f, ensure_ascii=False, indent=2)
            
            self.logger.logger.info(f"Scraped data saved to {output_path}")
            
        except Exception as e:
            self.logger.log_error_with_context(e, {'method': 'save_results', 'file': output_path})

# Добавим импорт для Path и signal
from pathlib import Path
import signal
import re # Добавлен импорт для регулярных выражений, используемых в PostProcessor

# Пример использования
if __name__ == "__main__":
    # Пример конфигурации
    config = ScrapingConfig(
        group_url="https://www.facebook.com/groups/yourgroupid", # ЗАМЕНИТЕ НА РЕАЛЬНЫЙ URL ГРУППЫ
        max_posts=10,
        scroll_delay=3.0,
        log_level=LogLevel.INFO,
        enable_gpu=False, # Установите True, если у вас есть GPU и хотите его использовать
        parallel_workers=2,
        cache_size=500,
        cache_ttl=1800,
        retry_attempts=5,
        retry_delay=2.0
    )

    scraper = EnhancedFacebookScraper(config)
    
    try:
        scraped_data = scraper.scrape()
        scraper.save_results(scraped_data)
        print(f"Scraped {len(scraped_data)} posts.")
        for post in scraped_data:
            print(f"Post URL: {post.post_url}")
            print(f"Author: {post.author.name}")
            print(f"Content: {post.content[:100]}...")
            print(f"Comments: {len(post.comments)}")
            print("-" * 50)
            
    except Exception as e:
        print(f"An error occurred during scraping: {e}")
        scraper.logger.logger.critical(f"Unhandled error in main execution: {e}", exc_info=True)