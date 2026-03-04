import json
from datetime import datetime, timezone
from typing import List

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, DateTime, Integer, Index, ForeignKey, insert, select, func, text

from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


engine = create_async_engine(f'postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}', echo=False)
AsyncSessionManager = async_sessionmaker(engine, expire_on_commit=False)

def now():
    """
    Функция для удобного получения дата-врмени с временной зоной UTC
    :return: datetime
    """
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Video(Base):
    __tablename__ = "videos"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # Здесь мы используем uuid4 в качестве уникального id
    creator_id: Mapped[str] = mapped_column(String(32))  # Хеш-строка айди креатора
    video_created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # Дата публикации видео
    views_count: Mapped[int] = mapped_column(Integer)  # Количество просмотров
    likes_count: Mapped[int] = mapped_column(Integer)  # Количество лайков
    comments_count: Mapped[int] = mapped_column(Integer)  # Количество комментариев
    reports_count: Mapped[int] = mapped_column(Integer)  # Количество репортов
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)  # Дата создания записи в базу данных
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # Дата обновления записи в базе

    # Связь видео со снепшотами One to Many
    snapshots: Mapped[List[Snapshot]] = relationship(back_populates='video', cascade='all, delete-orphan')

    # Хеш-индекс, чтобы было быстрее искать по id
    __table_args__ = (
        Index('idx_video_id_hash', 'id', postgresql_using='hash'),
    )


class Snapshot(Base):
    __tablename__ = "video_snapshots"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)  # Здесь используется хеш-строка в качестве уникального id
    video_id: Mapped[str] = mapped_column(ForeignKey('videos.id'), nullable=False)  # ID идео, к которому принадлежит снепшот
    views_count: Mapped[int] = mapped_column(Integer)  # Количество просмотров в момент снепшота
    likes_count: Mapped[int] = mapped_column(Integer)  # Количество лайков в момент снепшота
    comments_count: Mapped[int] = mapped_column(Integer)  # Количество комментариев в момент снепшота
    reports_count: Mapped[int] = mapped_column(Integer)  # Количество репортов в момент снепшота
    delta_views_count: Mapped[int] = mapped_column(Integer)  # Прибавление количества просмотров с последнего снепшота
    delta_likes_count: Mapped[int] = mapped_column(Integer)  # Прибавление количества лайков с последнего снепшота
    delta_comments_count: Mapped[int] = mapped_column(Integer)  # Прибавление количества комментариев с последнего снепшота
    delta_reports_count: Mapped[int] = mapped_column(Integer)  # Прибавление количества репортов с последнего снепшота
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)  # Дата создания записи в базу
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # Дата изменения записи в базе

    # Связь с видео, к которым принадлежат снепшоты Many to One
    video: Mapped[Video] = relationship(back_populates='snapshots')

    # Хеш-индекс для быстрого поиска по id
    __table_args__ = (
        Index('idx_snapshot_id_hash', 'id', postgresql_using='hash'),
    )


async def create_tables():
    """
    Функция создает все таблички, если они не были созданы
    :return:
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def load_data():
    """
    Функция загружает данные из файла `videos.json` в базу PostgreSQL,
    в случае если в таблице videos нет записей
    :return:
    """

    # Читаем файл videos.json
    with open('videos.json', 'r', encoding='utf-8') as file:
        raw_data = json.load(file)

    if not raw_data.get('videos'):
        raise Exception('Cannot find videos')

    # Списки, куда будем сохранять распарсенные данные
    raw_data = raw_data['videos']
    videos_data = []
    snapshots_data = []

    # Пробегаемся по сырым данным и собираем их в списки
    for video in raw_data:
        video_dict = {
            'id': video['id'],
            'creator_id': video['creator_id'],
            'video_created_at': datetime.fromisoformat(video['video_created_at']),
            'views_count': video['views_count'],
            'likes_count': video['likes_count'],
            'comments_count': video['comments_count'],
            'reports_count': video['reports_count'],
            'created_at': datetime.fromisoformat(video['created_at']),
            'updated_at': datetime.fromisoformat(video['updated_at']),
        }
        videos_data.append(video_dict)

        for snapshot in video['snapshots']:
            snapshot_dict = {
                'id': snapshot['id'],
                'video_id': snapshot['video_id'],
                'views_count': snapshot['views_count'],
                'likes_count': snapshot['likes_count'],
                'comments_count': snapshot['comments_count'],
                'reports_count': snapshot['reports_count'],
                'delta_views_count': snapshot['delta_views_count'],
                'delta_likes_count': snapshot['delta_likes_count'],
                'delta_comments_count': snapshot['delta_comments_count'],
                'delta_reports_count': snapshot['delta_reports_count'],
                'created_at': datetime.fromisoformat(snapshot['created_at']),
                'updated_at': datetime.fromisoformat(snapshot['updated_at']),
            }
            snapshots_data.append(snapshot_dict)

    # Загружаем данные в базу
    async with engine.begin() as conn:
        for i in range(0, len(videos_data), 100):
            await conn.execute(insert(Video).values(videos_data[i:i + 100]))
        for i in range(0, len(snapshots_data), 100):
            await conn.execute(insert(Snapshot).values(snapshots_data[i:i + 100]))


async def get_count_videos() -> int:
    """
    Фунция возвращает количество записей в таблице videos
    :return: int, число записей в таблице videos
    """
    async with engine.begin() as conn:
        response = await conn.scalar(select(func.count(Video.id)))
        return response


async def run_raw_sql(raw_sql: str) -> int:
    """
    Функция запускает сырой SQL запрос и возвращает скалярный результат
    :param raw_sql: строка с SQL кодом
    :return: результат выполнения
    """
    async with engine.begin() as conn:
        response = await conn.scalar(text(raw_sql))
    return response
