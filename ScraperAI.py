import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

class OpenRouterTester:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv('OPENROUTER_API_KEY')
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        
    def test_connection(self):
        """Тестирует подключение к OpenRouter API"""
        
        print("🔄 Тестирование подключения к OpenRouter API...")
        print("=" * 50)
        
        if not self.api_key:
            print("❌ ОШИБКА: API ключ не найден!")
            print("   Добавь OPENROUTER_API_KEY в файл .env или передай в конструктор")
            return False
            
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://www.facebook.com/groups/1075275215820713",  # Опционально
            "X-Title": "AI Scraper Bot"  # Опционально
        }
        
        # Простой тестовый запрос
        test_data = {
            "model": "deepseek/deepseek-chat-v3-0324:free",
            "messages": [
                {"role": "user", "content": "Привет! Это тест подключения. Ответь одним словом: работает"}
            ],
            "max_tokens": 10,
            "temperature": 0.1
        }
        
        try:
            print(f"📡 Отправляем запрос на {self.base_url}")
            print(f"🤖 Модель: {test_data['model']}")
            print(f"⏰ Время: {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 30)
            
            response = requests.post(
                self.base_url, 
                headers=headers, 
                json=test_data,
                timeout=30
            )
            
            # Проверяем статус ответа
            if response.status_code == 200:
                result = response.json()
                
                # Извлекаем ответ от AI
                ai_response = result["choices"][0]["message"]["content"]
                
                # Красивый вывод успеха
                print("✅ ПОДКЛЮЧЕНИЕ УСПЕШНО!")
                print("🎉 OpenRouter API работает отлично!")
                print(f"🤖 Ответ AI: '{ai_response.strip()}'")
                print(f"💰 Использовано токенов: {result.get('usage', {}).get('total_tokens', 'N/A')}")
                print(f"🏷️  Модель: {result.get('model', 'N/A')}")
                print("=" * 50)
                
                return True
                
            else:
                print(f"❌ ОШИБКА ПОДКЛЮЧЕНИЯ!")
                print(f"   Статус код: {response.status_code}")
                print(f"   Ответ: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            print("❌ ОШИБКА: Превышено время ожидания (30 сек)")
            return False
            
        except requests.exceptions.ConnectionError:
            print("❌ ОШИБКА: Проблемы с интернет-соединением")
            return False
            
        except json.JSONDecodeError:
            print("❌ ОШИБКА: Некорректный JSON в ответе")
            return False
            
        except Exception as e:
            print(f"❌ НЕОЖИДАННАЯ ОШИБКА: {str(e)}")
            return False
    
    def get_available_models(self):
        """Получает список доступных моделей"""
        
        print("\n🔍 Получаем список доступных моделей...")
        
        try:
            models_url = "https://openrouter.ai/api/v1/models"
            headers = {"Authorization": f"Bearer {self.api_key}"}
            
            response = requests.get(models_url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                models = response.json()
                print("📋 Доступные модели для скраппинга:")
                
                # Показываем только дешевые модели
                cheap_models = [
                    "deepseek/deepseek-chat",
                    "meta-llama/llama-3.2-3b-instruct:free",
                    "microsoft/phi-3-mini-128k-instruct:free",
                    "google/gemma-2-9b-it:free"
                ]
                
                for model in cheap_models:
                    print(f"   🤖 {model}")
                    
                return True
            else:
                print(f"   ⚠️  Не удалось получить список моделей: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ❌ Ошибка при получении моделей: {str(e)}")
            return False

def main():
    """Основная функция для тестирования"""
    
    print("🚀 AI SCRAPER - ТЕСТ ПОДКЛЮЧЕНИЯ")
    print("=" * 50)
    
    # Можно передать API ключ напрямую или использовать .env файл
    # tester = OpenRouterTester("your_api_key_here")
    tester = OpenRouterTester()
    
    # Тестируем подключение
    success = tester.test_connection()
    
    if success:
        # Если подключение успешно, показываем доступные модели
        tester.get_available_models()
        
        print("\n🎯 ГОТОВ К РАБОТЕ!")
        print("   Теперь можешь использовать AI для скраппинга")
        print("   Создай файл .env с твоим API ключом:")
        print("   OPENROUTER_API_KEY=your_key_here")
    else:
        print("\n🔧 ИНСТРУКЦИЯ ПО ИСПРАВЛЕНИЮ:")
        print("1. Зарегистрируйся на https://openrouter.ai")
        print("2. Получи API ключ в разделе Keys")
        print("3. Создай файл .env с ключом")
        print("4. Запусти скрипт снова")

if __name__ == "__main__":
    main()