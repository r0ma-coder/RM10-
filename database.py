import sqlite3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TaskDatabase:
    def __init__(self, db_name="tasks.db"):
        self.db_name = db_name
        self.init_database()
    
    def get_connection(self):
        """Получение соединения с базой данных"""
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row  # Для доступа к колонкам по имени
        return conn
    
    def init_database(self):
        """Инициализация базы данных и таблиц"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Таблица для задач
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parsing_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_link TEXT NOT NULL,
                limit_count INTEGER DEFAULT 300,
                status TEXT DEFAULT 'pending', -- pending, processing, completed, failed
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP NULL,
                completed_at TIMESTAMP NULL,
                result_filename TEXT NULL,
                users_found INTEGER DEFAULT 0,
                error_message TEXT NULL
            )
        ''')
        
        # Индексы для ускорения запросов
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON parsing_tasks(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON parsing_tasks(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON parsing_tasks(created_at)')
        
        conn.commit()
        conn.close()
        logger.info(f"База данных {self.db_name} инициализирована")
    
    def create_task(self, user_id, chat_link, limit_count):
        """Создание новой задачи"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Ограничиваем длину ссылки
        chat_link_short = chat_link[:200]
        
        cursor.execute('''
            INSERT INTO parsing_tasks (user_id, chat_link, limit_count, status)
            VALUES (?, ?, ?, 'pending')
        ''', (user_id, chat_link_short, limit_count))
        
        task_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        logger.info(f"Создана задача #{task_id} для user_id {user_id}")
        return task_id
    
    def get_pending_task(self):
        """Получение следующей задачи для обработки (только pending)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, user_id, chat_link, limit_count 
            FROM parsing_tasks 
            WHERE status = 'pending' 
            ORDER BY created_at ASC 
            LIMIT 1
        ''')
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            task = dict(row)
            logger.info(f"Найдена задача #{task['id']} для обработки")
            return task
        return None
    
    def get_task_info(self, task_id, user_id=None):
        """Получение информации о задаче"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if user_id:
            cursor.execute('''
                SELECT id, user_id, chat_link, limit_count, status,
                       created_at, started_at, completed_at,
                       users_found, error_message
                FROM parsing_tasks 
                WHERE id = ? AND user_id = ?
            ''', (task_id, user_id))
        else:
            cursor.execute('''
                SELECT id, user_id, chat_link, limit_count, status,
                       created_at, started_at, completed_at,
                       users_found, error_message
                FROM parsing_tasks 
                WHERE id = ?
            ''', (task_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return dict(row)
        return None
    
    def update_task_status(self, task_id, status, result_filename=None, users_found=0, error_message=None):
        """Обновление статуса задачи"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if status == 'processing':
                cursor.execute('''
                    UPDATE parsing_tasks 
                    SET status = ?, 
                        started_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, task_id))
            elif status == 'completed':
                # Ограничиваем длину имени файла
                filename_short = result_filename[:100] if result_filename else None
                cursor.execute('''
                    UPDATE parsing_tasks 
                    SET status = ?, 
                        completed_at = CURRENT_TIMESTAMP,
                        result_filename = ?,
                        users_found = ?
                    WHERE id = ?
                ''', (status, filename_short, users_found, task_id))
            elif status == 'failed':
                # Ограничиваем длину сообщения об ошибке
                error_short = error_message[:200] if error_message else None
                cursor.execute('''
                    UPDATE parsing_tasks 
                    SET status = ?, 
                        completed_at = CURRENT_TIMESTAMP,
                        error_message = ?
                    WHERE id = ?
                ''', (status, error_short, task_id))
            else:
                cursor.execute('''
                    UPDATE parsing_tasks 
                    SET status = ?
                    WHERE id = ?
                ''', (status, task_id))
            
            conn.commit()
            updated = cursor.rowcount > 0
            
            if updated:
                logger.info(f"Задача #{task_id} обновлена: статус={status}")
            else:
                logger.warning(f"Задача #{task_id} не была обновлена")
            
            return updated
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении задачи #{task_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def get_user_tasks(self, user_id, limit=10):
        """Получение задач пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, chat_link, limit_count, status, 
                   created_at, started_at, completed_at, 
                   users_found, error_message
            FROM parsing_tasks 
            WHERE user_id = ? 
            ORDER BY created_at DESC 
            LIMIT ?
        ''', (user_id, limit))
        
        tasks = []
        for row in cursor.fetchall():
            task_dict = dict(row)
            # Преобразуем timestamp в строку
            for time_field in ['created_at', 'started_at', 'completed_at']:
                if task_dict.get(time_field):
                    task_dict[time_field] = str(task_dict[time_field])
            tasks.append(task_dict)
        
        conn.close()
        return tasks
    
    def cleanup_old_tasks(self, days_old=7):
        """Очистка старых задач"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            DELETE FROM parsing_tasks 
            WHERE created_at < datetime('now', ?)
        ''', (f'-{days_old} days',))
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        if deleted_count > 0:
            logger.info(f"Удалено {deleted_count} старых задач (старше {days_old} дней)")
        
        return deleted_count

# Глобальный экземпляр базы данных
db = TaskDatabase()