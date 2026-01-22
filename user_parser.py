import asyncio
import logging
import time
import os
from telethon import TelegramClient, errors
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import Channel, ChatInviteExported
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

API_ID = 37780238 # Ваш api_id с my.telegram.org
API_HASH = 'fbfe8a419fea2f1ee79b9cc32bc49e18' # Ваш api_hash
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
    
    async def join_chat(self, chat_link):
        """Автоматическое вступление в чат/канал"""
        try:
            # Получаем сущность чата по ссылке
            chat = await self.client.get_entity(chat_link)
            chat_title = chat.title if hasattr(chat, 'title') else chat.username
            
            # Проверяем, является ли пользователь уже участником
            try:
                # Для каналов и супергрупп
                if isinstance(chat, Channel):
                    # Пытаемся получить информацию о канале
                    full_chat = await self.client(GetFullChannelRequest(channel=chat))
                    # Если мы участник, вернется информация
                    logger.info(f"✅ Уже состою в канале: {chat_title}")
                    return chat
            except errors.ChannelPrivateError:
                # Не участник канала - нужно вступить
                pass
            
            logger.info(f"🔄 Пытаюсь вступить в: {chat_title}")
            
            # Вступаем в чат/канал
            if hasattr(chat, 'username'):
                # Публичный чат/канал по username
                await self.client(JoinChannelRequest(channel=chat))
                logger.info(f"✅ Успешно вступил в публичный чат: {chat_title}")
            elif hasattr(chat, 'megagroup') and chat.megagroup:
                # Супергруппа
                await self.client(JoinChannelRequest(channel=chat))
                logger.info(f"✅ Успешно вступил в супергруппу: {chat_title}")
            else:
                # Обычный чат (может потребоваться инвайт-ссылка)
                try:
                    await self.client(JoinChannelRequest(channel=chat))
                    logger.info(f"✅ Успешно вступил в чат: {chat_title}")
                except errors.InviteHashEmptyError:
                    logger.error(f"❌ Для чата {chat_title} требуется инвайт-ссылка")
                    raise
                except errors.InviteHashExpiredError:
                    logger.error(f"❌ Инвайт-ссылка устарела для {chat_title}")
                    raise
                except errors.InviteHashInvalidError:
                    logger.error(f"❌ Неверная инвайт-ссылка для {chat_title}")
                    raise
            
            # Даем Telegram время обработать вступление
            await asyncio.sleep(2)
            return chat
            
        except errors.FloodWaitError as e:
            logger.warning(f"⏳ FloodWait при вступлении! Ждем {e.seconds} секунд...")
            await asyncio.sleep(e.seconds)
            return await self.join_chat(chat_link)
        except Exception as e:
            logger.error(f"❌ Ошибка при вступлении в чат: {e}")
            raise
    
    async def get_active_users(self, chat, max_users=300, min_messages=2):
        """Получает активных пользователей из чата"""
        active_users = []
        
        try:
            # Получаем всех участников чата
            logger.info(f"👥 Получаю список участников...")
            all_participants = await self.client.get_participants(chat)
            logger.info(f"📊 Всего участников: {len(all_participants)}")
            
            if len(all_participants) == 0:
                logger.warning("⚠️ В чате нет участников или нет доступа к списку")
                return []
            
            # Фильтруем пользователей с username и проверяем активность
            for i, user in enumerate(all_participants):
                if len(active_users) >= max_users:
                    break
                
                if not user.username:
                    continue
                
                try:
                    # Проверяем историю сообщений пользователя
                    messages = await self.client.get_messages(
                        chat, 
                        limit=50,  # Проверяем последние 50 сообщений
                        from_user=user
                    )
                    
                    user_msg_count = len(messages)
                    
                    if user_msg_count >= min_messages:
                        user_info = {
                            'id': user.id,
                            'username': user.username,
                            'first_name': user.first_name,
                            'last_name': user.last_name,
                            'messages_count': user_msg_count
                        }
                        active_users.append(user_info)
                        
                        logger.info(f"✅ Активный: @{user.username} (сообщений: {user_msg_count})")
                    
                    # Пауза между запросами, чтобы избежать блокировки
                    if i % 5 == 0:
                        await asyncio.sleep(1)
                        
                except Exception as e:
                    logger.debug(f"⚠️ Ошибка при проверке пользователя {user.username}: {e}")
                    continue
            
            logger.info(f"🎯 Найдено активных пользователей: {len(active_users)}")
            return active_users
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении пользователей: {e}")
            return []
    
    async def process_task(self, task):
        """Обработка одной задачи парсинга"""
        task_id = task['id']
        chat_link = task['chat_link']
        max_users = task['limit_count']
        
        logger.info(f"🔄 Начинаю обработку задачи #{task_id}: {chat_link}")
        
        try:
            # Шаг 1: Вступаем в чат
            chat = await self.join_chat(chat_link)
            chat_title = chat.title if hasattr(chat, 'title') else chat.username
            
            # Шаг 2: Получаем активных пользователей
            active_users = await self.get_active_users(chat, max_users, min_messages=2)
            
            # Шаг 3: Сохраняем результаты
            filename = await self.save_results(active_users, chat_title)
            
            if active_users:
                logger.info(f"✅ Задача #{task_id} завершена. Найдено: {len(active_users)}")
                return {
                    'success': True,
                    'filename': filename,
                    'users_found': len(active_users),
                    'chat_title': chat_title
                }
            else:
                logger.warning(f"⚠️ Задача #{task_id}: активные пользователи не найдены")
                return {
                    'success': True,
                    'filename': None,
                    'users_found': 0,
                    'chat_title': chat_title
                }
                
        except errors.ChannelPrivateError:
            logger.error(f"❌ Чат приватный: {chat_link}")
            return {
                'success': False,
                'error': 'Чат приватный. Требуется приглашение.'
            }
        except errors.InviteHashEmptyError:
            logger.error(f"❌ Требуется инвайт-ссылка: {chat_link}")
            return {
                'success': False,
                'error': 'Для вступления требуется инвайт-ссылка.'
            }
        except errors.UsernameNotOccupiedError:
            logger.error(f"❌ Чат не существует: {chat_link}")
            return {
                'success': False,
                'error': 'Чат/канал не существует.'
            }
        except errors.FloodWaitError as e:
            logger.error(f"⏳ FloodWait: {e.seconds} секунд")
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
        
        # Создаем безопасное имя файла
        safe_title = "".join(c for c in chat_title if c.isalnum() or c in (' ', '-', '_')).rstrip()
        timestamp = int(time.time())
        filename = f"results/{safe_title}_{timestamp}.txt"
        
        # Создаем папку results, если её нет
        os.makedirs("results", exist_ok=True)
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"Активные пользователи из '{chat_title}'\n")
                f.write(f"Всего найдено: {len(users)}\n")
                f.write(f"Время парсинга: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 60 + "\n\n")
                
                # Сортируем по количеству сообщений
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
                # Получаем следующую задачу
                task = db.get_pending_task()
                
                if task:
                    task_id = task['id']
                    logger.info(f"📋 Найдена задача #{task_id} для обработки")
                    
                    # Обновляем статус задачи
                    db.update_task_status(task_id, 'processing')
                    
                    # Обрабатываем задачу
                    result = await self.process_task(task)
                    
                    # Обновляем статус в зависимости от результата
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
                            logger.info(f"ℹ️ Задача #{task_id} завершена (нет активных пользователей)")
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        db.update_task_status(
                            task_id, 
                            'failed',
                            error_message=error_msg[:100]
                        )
                        logger.error(f"❌ Задача #{task_id} завершилась с ошибкой: {error_msg}")
                        
                        # Если FloodWait, делаем паузу
                        if 'FloodWait' in error_msg:
                            wait_time = result.get('retry_after', 60)
                            logger.warning(f"⏳ Пауза {wait_time} секунд из-за FloodWait...")
                            await asyncio.sleep(wait_time)
                else:
                    # Нет задач - ждём
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

# Необходимый импорт
from telethon.tl.functions.channels import GetFullChannelRequest

# --- Запуск парсера ---
async def main():
    worker = ParserWorker()
    await worker.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Парсер остановлен пользователем")