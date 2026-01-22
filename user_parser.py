import asyncio
import logging
import time
import os
from telethon import TelegramClient, errors
from database import db

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

API_ID = 37780238  # ВАШ_API_ID
API_HASH = 'fbfe8a419fea2f1ee79b9cc32bc49e18'  # ВАШ_API_HASH
PHONE_NUMBER = '+959760950133'  # Номер аккаунта для парсера

class ParserWorker:
    def __init__(self):
        self.client = None
        self.is_running = True
        self.session_file = 'parser_session.session'
    
    async def initialize_client(self):
        """Инициализация клиента Telegram"""
        try:
            self.client = TelegramClient(self.session_file, API_ID, API_HASH)
            await self.client.connect()
            logger.info("Подключение к Telegram установлено")
            
            if not await self.client.is_user_authorized():
                logger.info("Сессия не авторизована. Запрашиваю код...")
                await self.client.send_code_request(PHONE_NUMBER)
                code = input("📱 Введите код из Telegram: ")
                
                try:
                    await self.client.sign_in(PHONE_NUMBER, code)
                except errors.SessionPasswordNeededError:
                    password = input("🔐 Требуется пароль двухфакторной аутентификации: ")
                    await self.client.sign_in(password=password)
            
            logger.info("✅ Клиент Telegram успешно авторизован")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации клиента: {e}")
            return False
    
    async def join_chat_or_channel(self, chat_link):
        """
        Пытается вступить в чат или канал по ссылке.
        Возвращает объект чата в случае успеха.
        """
        try:
            logger.info(f"🔄 Пытаюсь вступить в чат/канал: {chat_link}")
            
            # 1. Сначала получаем объект чата по ссылке
            try:
                chat = await self.client.get_entity(chat_link)
            except Exception as e:
                logger.error(f"❌ Не могу найти чат по ссылке {chat_link}: {e}")
                return None
            
            chat_title = chat.title if hasattr(chat, 'title') else chat.username
            
            # 2. Пытаемся вступить
            try:
                # Универсальный метод для вступления в чаты и каналы
                await self.client.join_chat(chat_link)
                logger.info(f"✅ Успешно вступил в: {chat_title}")
                return chat
            except errors.UserAlreadyParticipantError:
                logger.info(f"ℹ️ Уже состою в чате: {chat_title}")
                return chat
            except errors.InviteHashExpiredError:
                logger.error(f"❌ Срок действия ссылки-приглашения истек: {chat_link}")
                return None
            except errors.ChannelPrivateError:
                logger.error(f"❌ Чат приватный и нет приглашения: {chat_title}")
                return None
            except errors.InviteRequestSentError:
                logger.warning(f"⚠️ Заявка на вступление в '{chat_title}' отправлена. Нужно ждать подтверждения.")
                return chat  # Возвращаем чат, но парсинг, скорее всего, не сработает
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при вступлении в '{chat_title}': {e}. Пробую продолжить...")
                return chat  # Пробуем продолжить, даже если не удалось вступить
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при вступлении в чат: {e}")
            return None
    
    async def get_active_users(self, chat, max_users=300, min_messages=2):
        """
        Основная функция парсинга активных пользователей.
        Анализирует историю сообщений чата.
        """
        active_users = {}
        total_messages_checked = 0
        
        try:
            logger.info(f"📊 Начинаю анализ истории сообщений...")
            
            # Получаем историю сообщений (до 1000 сообщений)
            offset_id = 0
            batch_count = 0
            
            while total_messages_checked < 1000 and len(active_users) < max_users:
                try:
                    # Получаем пачку сообщений
                    messages = await self.client.get_messages(
                        chat, 
                        limit=100,
                        offset_id=offset_id
                    )
                    
                    if not messages:
                        logger.info("📭 Больше сообщений нет")
                        break
                    
                    batch_count += 1
                    total_messages_checked += len(messages)
                    
                    # Анализируем отправителей в этой пачке
                    for msg in messages:
                        if hasattr(msg, 'sender_id') and msg.sender_id:
                            try:
                                sender = await self.client.get_entity(msg.sender_id)
                                
                                # Нас интересуют только пользователи с username
                                if hasattr(sender, 'username') and sender.username:
                                    username = sender.username.lower()
                                    
                                    if username not in active_users:
                                        active_users[username] = {
                                            'id': sender.id,
                                            'username': sender.username,
                                            'first_name': getattr(sender, 'first_name', ''),
                                            'last_name': getattr(sender, 'last_name', ''),
                                            'messages_count': 1
                                        }
                                    else:
                                        active_users[username]['messages_count'] += 1
                            except Exception as e:
                                logger.debug(f"Не удалось обработать отправителя: {e}")
                                continue
                    
                    # Обновляем offset_id для следующей пачки
                    offset_id = messages[-1].id
                    
                    logger.info(f"📈 Обработано сообщений: {total_messages_checked}, "
                               f"Найдено уникальных пользователей: {len(active_users)}")
                    
                    # Пауза между запросами для избежания FloodWait
                    if batch_count % 5 == 0:
                        await asyncio.sleep(2)
                        
                except errors.FloodWaitError as e:
                    logger.warning(f"⏳ FloodWait! Ждем {e.seconds} секунд...")
                    await asyncio.sleep(e.seconds)
                    continue
                except Exception as e:
                    logger.error(f"❌ Ошибка при получении сообщений: {e}")
                    break
            
            # Фильтруем только активных пользователей (2+ сообщений)
            result = []
            for username, user_data in active_users.items():
                if user_data['messages_count'] >= min_messages:
                    result.append(user_data)
            
            logger.info(f"✅ Найдено активных пользователей (2+ сообщений): {len(result)}")
            logger.info(f"📋 Проанализировано сообщений: {total_messages_checked}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка при анализе истории чата: {e}")
            return []
    
    async def process_task(self, task):
        """Обработка одной задачи парсинга"""
        task_id = task['id']
        chat_link = task['chat_link']
        max_users = task['limit_count']
        
        logger.info(f"🔄 Начинаю обработку задачи #{task_id}: {chat_link}")
        
        try:
            # 1. Вступаем в чат/канал
            chat = await self.join_chat_or_channel(chat_link)
            if not chat:
                return {
                    'success': False,
                    'error': 'Не удалось вступить в чат/канал. Проверьте ссылку.'
                }
            
            chat_title = chat.title if hasattr(chat, 'title') else chat.username
            logger.info(f"📁 Целевой чат: {chat_title}")
            
            # 2. Парсим активных пользователей
            active_users = await self.get_active_users(chat, max_users, min_messages=2)
            
            # 3. Сохраняем результаты
            filename = await self.save_results(active_users, chat_title)
            
            if active_users:
                logger.info(f"✅ Задача #{task_id} завершена. Найдено активных: {len(active_users)}")
                return {
                    'success': True,
                    'filename': filename,
                    'users_found': len(active_users),
                    'chat_title': chat_title
                }
            else:
                logger.warning(f"⚠️ Задача #{task_id}: активные пользователи не найдены")
                logger.info(f"ℹ️ Возможные причины для '{chat_title}':")
                logger.info("  - Пользователи пишут без username (с телефона)")
                logger.info("  - В истории меньше 2 сообщений от каждого пользователя")
                logger.info("  - Чат очень новый или неактивный")
                return {
                    'success': True,
                    'filename': None,
                    'users_found': 0,
                    'chat_title': chat_title,
                    'note': 'Активные пользователи не найдены'
                }
                
        except errors.FloodWaitError as e:
            logger.error(f"⏳ FloodWaitError для задачи #{task_id}: {e.seconds} секунд")
            return {
                'success': False,
                'error': f'FloodWait: {e.seconds} секунд',
                'retry_after': e.seconds
            }
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке задачи #{task_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def save_results(self, users, chat_title):
        """Сохраняет результаты в файл"""
        if not users:
            return None
        
        safe_title = "".join(c for c in chat_title if c.isalnum() or c in (' ', '-', '_')).rstrip()
        timestamp = int(time.time())
        filename = f"results/{safe_title}_{timestamp}.txt"
        
        os.makedirs("results", exist_ok=True)
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"Активные пользователи из '{chat_title}'\n")
                f.write(f"Всего найдено: {len(users)}\n")
                f.write(f"Время парсинга: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 60 + "\n\n")
                
                users_sorted = sorted(users, key=lambda x: x['messages_count'], reverse=True)
                
                for i, user in enumerate(users_sorted, 1):
                    full_name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
                    f.write(f"{i:3}. @{user['username']:20} ")
                    f.write(f"- {full_name:20} ")
                    f.write(f"(сообщений: {user['messages_count']:3})\n")
            
            logger.info(f"💾 Результаты сохранены в {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения файла: {e}")
            return None
    
    async def worker_loop(self):
        """Основной цикл работника"""
        logger.info("🚀 Парсер запущен и ожидает задачи...")
        
        while self.is_running:
            try:
                task = db.get_pending_task()
                
                if task:
                    task_id = task['id']
                    logger.info(f"📋 Найдена задача #{task_id} для обработки")
                    
                    db.update_task_status(task_id, 'processing')
                    
                    result = await self.process_task(task)
                    
                    if result['success']:
                        if result.get('users_found', 0) > 0:
                            db.update_task_status(
                                task_id, 
                                'completed',
                                result_filename=result.get('filename'),
                                users_found=result.get('users_found', 0)
                            )
                            logger.info(f"✅ Задача #{task_id} успешно завершена")
                        else:
                            db.update_task_status(
                                task_id, 
                                'completed',
                                result_filename=None,
                                users_found=0
                            )
                            logger.info(f"ℹ️ Задача #{task_id} завершена")
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        db.update_task_status(
                            task_id, 
                            'failed',
                            error_message=error_msg[:100]
                        )
                        logger.error(f"❌ Задача #{task_id} завершилась с ошибкой: {error_msg}")
                        
                        if 'FloodWait' in error_msg:
                            wait_time = result.get('retry_after', 60)
                            logger.warning(f"⏳ Пауза {wait_time} секунд из-за FloodWait...")
                            await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(5)
                    
            except KeyboardInterrupt:
                logger.info("🛑 Получен сигнал прерывания")
                self.is_running = False
            except Exception as e:
                logger.error(f"❌ Критическая ошибка в основном цикле: {e}")
                await asyncio.sleep(30)
    
    async def start(self):
        """Запуск работника"""
        if not await self.initialize_client():
            logger.error("❌ Не удалось инициализировать клиент Telegram")
            return False
        
        logger.info("✅ Парсер готов к работе")
        
        try:
            await self.worker_loop()
        finally:
            if self.client and self.client.is_connected():
                await self.client.disconnect()
                logger.info("📴 Соединение с Telegram закрыто")
        
        return True

# --- Запуск парсера ---
async def main():
    worker = ParserWorker()
    await worker.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Парсер остановлен пользователем")