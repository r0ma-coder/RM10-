import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardRemove
from aiogram.enums import ParseMode
from database import db

# --- Настройки ---
BOT_TOKEN = "8457649746:AAFqlHpszZisrBS21VrMeJrknen6PHtNHHk"  # Замените на токен от @BotFather

# --- Инициализация ---
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# --- Класс состояний FSM ---
class ParserStates(StatesGroup):
    waiting_for_link = State()
    waiting_for_limit = State()

# ========== ОБРАБОТЧИКИ КОМАНД ==========

# Команда /start
# В команде /tasks добавьте более подробные сообщения:

@dp.message(Command("tasks"))
async def cmd_tasks(message: types.Message):
    user_tasks = db.get_user_tasks(message.from_user.id, limit=10)
    
    if not user_tasks:
        await message.answer("📭 <b>У вас пока нет задач.</b>\n\nИспользуйте /start чтобы создать первую задачу.")
        return
    
    tasks_text = "<b>📋 Ваши последние задачи:</b>\n\n"
    
    for task in user_tasks:
        status_icons = {
            'pending': '⏳',
            'processing': '🔄',
            'completed': '✅',
            'failed': '❌'
        }
        
        icon = status_icons.get(task['status'], '📌')
        created_time = task['created_at'][:19] if task['created_at'] else 'N/A'
        
        tasks_text += f"{icon} <b>Задача #{task['id']}</b>\n"
        tasks_text += f"📎 Ссылка: <code>{task['chat_link'][:30]}...</code>\n"
        tasks_text += f"🔢 Лимит: <b>{task['limit_count']}</b>\n"
        tasks_text += f"📊 Статус: <b>{task['status']}</b>\n"
        
        if task['status'] == 'completed':
            if task['users_found'] > 0:
                tasks_text += f"👥 Найдено: <b>{task['users_found']}</b> пользователей\n"
                if task['result_filename']:
                    tasks_text += f"💾 Файл: <code>{task['result_filename']}</code>\n"
            else:
                tasks_text += f"👥 Найдено: <b>0</b> пользователей\n"
                tasks_text += f"<i>Возможные причины:</i>\n"
                tasks_text += f"• Пользователи без username\n"
                tasks_text += f"• Мало сообщений (<2)\n"
                tasks_text += f"• Нет доступа к чату\n"
        elif task['status'] == 'failed' and task['error_message']:
            tasks_text += f"⚠️ Ошибка: <i>{task['error_message']}</i>\n"
        
        tasks_text += f"🕐 Создана: <i>{created_time}</i>\n"
        tasks_text += "─" * 30 + "\n"
    
    tasks_text += f"\n<b>Всего задач:</b> {len(user_tasks)}"
    tasks_text += f"\n\n<i>Примечание: Для парсинга бот автоматически вступает в чат.</i>"
    
    await message.answer(tasks_text)

# Команда /help
@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    help_text = (
        "<b>ℹ️ Помощь по использованию бота:</b>\n\n"
        "1. Отправьте ссылку на публичный чат/канал\n"
        "2. Укажите лимит (0 для максимума 300, или число от 1 до 300)\n"
        "3. Бот добавит задачу в очередь парсинга\n"
        "4. Когда парсинг завершится, бот сохранит результат\n\n"
        "<b>📋 Для просмотра статуса задач используйте /tasks</b>\n\n"
        "<i>Примечание: Парсинг может занять некоторое время в зависимости от размера чата.</i>"
    )
    await message.answer(help_text)

# Команда /tasks
@dp.message(Command("tasks"))
async def cmd_tasks(message: types.Message):
    """Показывает последние задачи пользователя"""
    user_tasks = db.get_user_tasks(message.from_user.id, limit=10)
    
    if not user_tasks:
        await message.answer("📭 <b>У вас пока нет задач.</b>\n\nИспользуйте /start чтобы создать первую задачу.")
        return
    
    tasks_text = "<b>📋 Ваши последние задачи:</b>\n\n"
    
    for task in user_tasks:
        # Иконки статусов
        status_icons = {
            'pending': '⏳',
            'processing': '🔄',
            'completed': '✅',
            'failed': '❌'
        }
        
        icon = status_icons.get(task['status'], '📌')
        
        # Форматируем время
        created_time = task['created_at'][:19] if task['created_at'] else 'N/A'
        
        tasks_text += f"{icon} <b>Задача #{task['id']}</b>\n"
        tasks_text += f"<code>{task['chat_link'][:30]}</code>\n"
        tasks_text += f"Лимит: <b>{task['limit_count']}</b>\n"
        tasks_text += f"Статус: <b>{task['status']}</b>\n"
        
        if task['status'] == 'completed' and task['users_found'] > 0:
            tasks_text += f"Найдено: <b>{task['users_found']}</b> пользователей\n"
        elif task['status'] == 'failed' and task['error_message']:
            tasks_text += f"Ошибка: <i>{task['error_message'][:50]}</i>\n"
        
        tasks_text += f"Создана: <i>{created_time}</i>\n"
        tasks_text += "─" * 30 + "\n"
    
    tasks_text += f"\n<b>Всего задач:</b> {len(user_tasks)}"
    
    await message.answer(tasks_text)

# Команда /cancel
@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("❌ Нет активных действий для отмены.")
        return
    
    await state.clear()
    await message.answer("✅ Текущее действие отменено. Используйте /start чтобы начать заново.")

# ========== ОБРАБОТЧИКИ СОСТОЯНИЙ ==========

# Принятие ссылки
@dp.message(ParserStates.waiting_for_link)
async def process_link(message: types.Message, state: FSMContext):
    user_link = message.text.strip()
    
    # Обработка команды /cancel в состоянии
    if user_link.lower() == '/cancel':
        await cmd_cancel(message, state)
        return
    
    # Валидация ссылки
    if not (user_link.startswith("https://t.me/") or user_link.startswith("@")):
        await message.answer(
            "❌ <b>Неверный формат ссылки!</b>\n\n"
            "Пожалуйста, отправьте ссылку в одном из форматов:\n"
            "• <code>https://t.me/username</code>\n"
            "• <code>@username</code>\n\n"
            "Или отправьте /cancel для отмены."
        )
        return
    
    # Сохраняем ссылку
    await state.update_data(chat_link=user_link)
    
    limit_text = (
        "<b>🔢 Установите ограничение количества юзернеймов:</b>\n\n"
        "• <b>0</b> - без ограничения (максимум 300)\n"
        "• <b>1-300</b> - конкретное количество\n\n"
        "<i>Просто отправьте цифру:</i>"
    )
    
    await message.answer(limit_text)
    await state.set_state(ParserStates.waiting_for_limit)

# Принятие лимита
@dp.message(ParserStates.waiting_for_limit)
async def process_limit(message: types.Message, state: FSMContext):
    user_input = message.text.strip()
    
    # Обработка команды /cancel в состоянии
    if user_input.lower() == '/cancel':
        await cmd_cancel(message, state)
        return
    
    # Проверяем, что введено число
    if not user_input.isdigit():
        await message.answer(
            "❌ <b>Некорректный ввод!</b>\n\n"
            "Пожалуйста, введите только цифру:\n"
            "• <b>0</b> - без ограничения\n"
            "• <b>1-300</b> - конкретное количество\n\n"
            "Или отправьте /cancel для отмены."
        )
        return
    
    limit = int(user_input)
    
    # Проверяем диапазон
    if limit > 300:
        await message.answer(
            "❌ <b>Слишком большое ограничение!</b>\n\n"
            "Максимальное значение: <b>300</b>\n"
            "Введите число от <b>0</b> до <b>300</b>:\n\n"
            "Или отправьте /cancel для отмены."
        )
        return
    
    # Получаем сохранённую ссылку
    data = await state.get_data()
    chat_link = data.get("chat_link")
    
    # Определяем итоговый лимит
    final_limit = 300 if limit == 0 else limit
    limit_message = "без ограничения (максимум 300)" if limit == 0 else f"не более {final_limit}"
    
    # Сохраняем задачу в базу данных
    try:
        task_id = db.create_task(
            user_id=message.from_user.id,
            chat_link=chat_link,
            limit_count=final_limit
        )
        
        result_text = (
            f"✅ <b>Задача #{task_id} создана!</b>\n\n"
            f"📎 <b>Ссылка:</b> <code>{chat_link}</code>\n"
            f"🔢 <b>Ограничение:</b> {limit_message}\n"
            f"👤 <b>Пользователь:</b> {message.from_user.full_name}\n\n"
            "<b>⏳ Задача поставлена в очередь на парсинг.</b>\n"
            "Используйте /tasks для отслеживания статуса."
        )
        
        # Логируем создание задачи
        logging.info(f"User {message.from_user.id} создал задачу #{task_id} для {chat_link}")
        
    except Exception as e:
        logging.error(f"Ошибка при создании задачи: {e}")
        result_text = (
            "❌ <b>Ошибка при создании задачи!</b>\n\n"
            "Попробуйте позже или обратитесь к администратору."
        )
    
    await message.answer(result_text, reply_markup=ReplyKeyboardRemove())
    await state.clear()

# ========== ОБРАБОТЧИК ВСЕХ ОСТАЛЬНЫХ СООБЩЕНИЙ ==========

@dp.message()
async def handle_other_messages(message: types.Message):
    """Обработка всех остальных сообщений"""
    # Если это не команда, просто показываем приветствие
    await message.answer(
        "🤔 <b>Не понял вашу команду.</b>\n\n"
        "Используйте /start чтобы начать создание задачи парсинга.\n"
        "Или /help для получения справки."
    )

# ========== ЗАПУСК БОТА ==========

async def main():
    # Настраиваем логирование
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    # Проверяем подключение к базе данных
    try:
        # Простой тест подключения
        conn = db.get_connection()
        conn.close()
        logging.info("✅ Подключение к базе данных успешно")
    except Exception as e:
        logging.error(f"❌ Ошибка подключения к базе данных: {e}")
        return
    
    # Запускаем бота
    logging.info("🚀 Бот запускается...")
    
    # Сброс обновлений (удаляет старые сообщения в очереди)
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Запускаем опрос
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())