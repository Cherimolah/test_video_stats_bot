import json
from datetime import datetime
from typing import List

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, DateTime, Integer, Index, ForeignKey, insert, select, func, text

from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


engine = create_async_engine(f'postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}', echo=False)
AsyncSessionManager = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Video(Base):
    __tablename__ = "videos"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # In this case we use uuid4 as unique id
    creator_id: Mapped[str] = mapped_column(String(32))
    video_created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    views_count: Mapped[int] = mapped_column(Integer)
    likes_count: Mapped[int] = mapped_column(Integer)
    comments_count: Mapped[int] = mapped_column(Integer)
    reports_count: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    snapshots: Mapped[List[Snapshot]] = relationship(back_populates='video', cascade='all, delete-orphan')

    __table_args__ = (
        Index('idx_video_id_hash', 'id', postgresql_using='hash'),
    )


class Snapshot(Base):
    __tablename__ = "video_snapshots"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)  # Here we use hash as unique id
    video_id: Mapped[str] = mapped_column(ForeignKey('videos.id'), nullable=False)
    views_count: Mapped[int] = mapped_column(Integer)
    likes_count: Mapped[int] = mapped_column(Integer)
    comments_count: Mapped[int] = mapped_column(Integer)
    reports_count: Mapped[int] = mapped_column(Integer)
    delta_views_count: Mapped[int] = mapped_column(Integer)
    delta_likes_count: Mapped[int] = mapped_column(Integer)
    delta_comments_count: Mapped[int] = mapped_column(Integer)
    delta_reports_count: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    video: Mapped[Video] = relationship(back_populates='snapshots')

    __table_args__ = (
        Index('idx_snapshot_id_hash', 'id', postgresql_using='hash'),
    )


async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def load_data():
    with open('videos.json', 'r', encoding='utf-8') as file:
        raw_data = json.load(file)

    if not raw_data.get('videos'):
        raise Exception('Cannot find videos')

    raw_data = raw_data['videos']
    videos_data = []
    snapshots_data = []

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

    async with engine.begin() as conn:
        for i in range(0, len(videos_data), 100):
            await conn.execute(insert(Video).values(videos_data[i:i + 100]))
        for i in range(0, len(snapshots_data), 100):
            await conn.execute(insert(Snapshot).values(snapshots_data[i:i + 100]))


async def get_count_videos():
    async with engine.begin() as conn:
        response = await conn.scalar(select(func.count(Video.id)))
        return response


async def run_raw_sql(raw_sql: str) -> int:
    async with engine.begin() as conn:
        response = await conn.scalar(text(raw_sql))
    return response
