"""
server.py — Сервер для онлайн-игры «Эрудит»
Запуск: python server.py
Порт:  8765
"""
import asyncio
import json
import logging
import os
import random
import string
import time

import websockets
from websockets.server import WebSocketServerProtocol

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger('erudit')

# ── Структуры данных ──────────────────────────────────────────────────────────

class Room:
    """Комната на двух игроков."""
    def __init__(self, room_id: str):
        self.id          = room_id
        self.players:  list[WebSocketServerProtocol] = []   # [ws_p1, ws_p2]
        self.names:    dict[int, str]                = {}   # {1: name, 2: name}
        self.state:    dict | None                   = None # GameState dict
        self.created   = time.time()
        self.started   = False

    @property
    def full(self) -> bool:
        return len(self.players) == 2

    def other(self, ws: WebSocketServerProtocol) -> WebSocketServerProtocol | None:
        """Вернуть сокет второго игрока."""
        for p in self.players:
            if p is not ws:
                return p
        return None

    def player_num(self, ws: WebSocketServerProtocol) -> int | None:
        for i, p in enumerate(self.players):
            if p is ws:
                return i + 1
        return None


rooms: dict[str, Room] = {}   # room_id → Room


def make_room_id(length: int = 5) -> str:
    while True:
        rid = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
        if rid not in rooms:
            return rid


def cleanup_old_rooms() -> None:
    """Удаляем комнаты старше 2 часов или без игроков."""
    now = time.time()
    dead = [rid for rid, r in rooms.items()
            if now - r.created > 7200 or len(r.players) == 0]
    for rid in dead:
        del rooms[rid]
        log.info(f"Комната {rid} удалена (устарела)")


# ── Отправка сообщений ────────────────────────────────────────────────────────

async def send(ws: WebSocketServerProtocol, msg: dict) -> None:
    try:
        await ws.send(json.dumps(msg, ensure_ascii=False))
    except Exception:
        pass


async def broadcast(room: Room, msg: dict, exclude: WebSocketServerProtocol | None = None) -> None:
    for p in room.players:
        if p is not exclude:
            await send(p, msg)


# ── Обработчик подключения ────────────────────────────────────────────────────

async def handler(ws: WebSocketServerProtocol) -> None:
    room: Room | None = None
    player_num: int | None = None

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await send(ws, {'type': 'error', 'text': 'bad json'})
                continue

            mtype = msg.get('type')

            # ── CREATE: создать новую комнату ─────────────────────────────────
            if mtype == 'create':
                cleanup_old_rooms()
                room_id  = make_room_id()
                room     = Room(room_id)
                rooms[room_id] = room
                room.players.append(ws)
                room.names[1] = msg.get('name', 'Παίκτης 1')
                player_num = 1
                log.info(f"Создана комната {room_id}  игрок: {room.names[1]}")
                await send(ws, {
                    'type':       'created',
                    'room_id':    room_id,
                    'player_num': 1,
                })

            # ── JOIN: подключиться к существующей комнате ─────────────────────
            elif mtype == 'join':
                room_id = msg.get('room_id', '').strip().upper()
                if room_id not in rooms:
                    await send(ws, {'type': 'error', 'text': 'room_not_found'})
                    continue
                r = rooms[room_id]
                if r.full:
                    await send(ws, {'type': 'error', 'text': 'room_full'})
                    continue
                room = r
                room.players.append(ws)
                room.names[2] = msg.get('name', 'Παίκτης 2')
                player_num = 2
                log.info(f"Комната {room_id}: подключился {room.names[2]}")

                # Уведомляем обоих — старт
                await send(ws, {
                    'type':       'joined',
                    'player_num': 2,
                    'room_id':    room_id,
                    'opponent':   room.names[1],
                })
                await send(room.players[0], {
                    'type':     'opponent_joined',
                    'opponent': room.names[2],
                })
                room.started = True

            # ── STATE: игрок отправляет новое состояние игры после хода ───────
            elif mtype == 'state':
                if room is None:
                    continue
                room.state = msg.get('state')
                other = room.other(ws)
                if other:
                    await send(other, {
                        'type':  'state',
                        'state': room.state,
                    })

            # ── CHAT: простое сообщение ────────────────────────────────────────
            elif mtype == 'chat':
                if room is None:
                    continue
                other = room.other(ws)
                if other:
                    await send(other, {
                        'type': 'chat',
                        'text': msg.get('text', '')[:200],
                        'from': room.names.get(player_num, '?'),
                    })

            # ── REMATCH: реванш ───────────────────────────────────────────────
            elif mtype == 'rematch':
                if room is None:
                    continue
                # Помечаем что этот игрок хочет реванш
                if not hasattr(room, 'rematch_votes'):
                    room.rematch_votes = set()
                room.rematch_votes.add(player_num)
                other = room.other(ws)
                if other:
                    # Уведомляем соперника о предложении
                    await send(other, {'type': 'rematch_request'})
                # Оба согласились — старт реванша
                if len(room.rematch_votes) == 2:
                    room.rematch_votes = set()
                    room.state = None
                    await broadcast(room, {'type': 'rematch_start'})
                    log.info(f"Комната {room.id}: реванш начался")

            # ── PING: keepalive ───────────────────────────────────────────────
            elif mtype == 'ping':
                await send(ws, {'type': 'pong'})

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if room:
            if ws in room.players:
                room.players.remove(ws)
            other = room.other(ws)
            if other:
                await send(other, {
                    'type': 'opponent_disconnected',
                    'text': f"{room.names.get(player_num, '?')} αποσυνδέθηκε",
                })
            log.info(f"Комната {room.id}: игрок {player_num} отключился")


# ── Запуск ────────────────────────────────────────────────────────────────────

async def main():
    port = int(os.environ.get('PORT', 8765))
    log.info(f"Сервер запущен на порту {port}")
    async with websockets.serve(handler, '0.0.0.0', port):
        await asyncio.Future()  # работаем вечно


if __name__ == '__main__':
    asyncio.run(main())
