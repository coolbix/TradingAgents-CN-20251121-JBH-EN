"""WebSocket Notification System
Substitute SSE + Redis PubSub to address leak connections
"""
import asyncio
import json
import logging
from typing import Dict, Set
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, HTTPException
from datetime import datetime

from app.services.auth_service import AuthService

router = APIRouter()
logger = logging.getLogger("webapi.websocket")

#Global WebSocket Connection Manager
class ConnectionManager:
    """WebSocket Connection Manager"""
    
    def __init__(self):
        # user_id -> Set[WebSocket]
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket, user_id: str):
        """Connect WebSocket"""
        await websocket.accept()
        
        async with self._lock:
            if user_id not in self.active_connections:
                self.active_connections[user_id] = set()
            self.active_connections[user_id].add(websocket)
            
            total_connections = sum(len(conns) for conns in self.active_connections.values())
            logger.info(f"[WS] New connection:{user_id}, "
                       f"Number of connections ={len(self.active_connections[user_id])}, "
                       f"Total number of connections ={total_connections}")
    
    async def disconnect(self, websocket: WebSocket, user_id: str):
        """Break WebSocket"""
        async with self._lock:
            if user_id in self.active_connections:
                self.active_connections[user_id].discard(websocket)
                if not self.active_connections[user_id]:
                    del self.active_connections[user_id]
            
            total_connections = sum(len(conns) for conns in self.active_connections.values())
            logger.info(f"[WS] Disconnected: user={user_id}, total connection ={total_connections}")
    
    async def send_personal_message(self, message: dict, user_id: str):
        """Can not open message"""
        async with self._lock:
            if user_id not in self.active_connections:
                logger.debug(f"[WS] User{user_id}No active connection")
                return
            
            connections = list(self.active_connections[user_id])
        
        #Sending messages outside the lock. Avoid blocking.
        message_json = json.dumps(message, ensure_ascii=False)
        dead_connections = []
        
        for connection in connections:
            try:
                await connection.send_text(message_json)
                logger.debug(f"[WS] Sending message to user={user_id}")
            except Exception as e:
                logger.warning(f"[WS] Sending message failed:{e}")
                dead_connections.append(connection)
        
        #Clean Dead Connection
        if dead_connections:
            async with self._lock:
                if user_id in self.active_connections:
                    for conn in dead_connections:
                        self.active_connections[user_id].discard(conn)
                    if not self.active_connections[user_id]:
                        del self.active_connections[user_id]
    
    async def broadcast(self, message: dict):
        """Radio message to all connections"""
        async with self._lock:
            all_connections = []
            for connections in self.active_connections.values():
                all_connections.extend(connections)
        
        message_json = json.dumps(message, ensure_ascii=False)
        
        for connection in all_connections:
            try:
                await connection.send_text(message_json)
            except Exception as e:
                logger.warning(f"[WS] Broadcast failed:{e}")
    
    def get_stats(self) -> dict:
        """Get Connect Statistics"""
        return {
            "total_users": len(self.active_connections),
            "total_connections": sum(len(conns) for conns in self.active_connections.values()),
            "users": {user_id: len(conns) for user_id, conns in self.active_connections.items()}
        }


#Examples of global connection manager
manager = ConnectionManager()


@router.websocket("/ws/notifications")
async def websocket_notifications_endpoint(
    websocket: WebSocket,
    token: str = Query(...)
):
    """WebSocket Notifierend Points

    Client connection: ws://localhost: 8,000/api/ws/notifications?token=<jwt token>

    Message format:
    FMT 0 
    ♪ I'm sorry ♪
    """
    #Authentication token
    token_data = AuthService.verify_token(token)
    if not token_data:
        await websocket.close(code=1008, reason="Unauthorized")
        return
    
    user_id = "admin"  #Fetch from token data
    
    #Connect WebSocket
    await manager.connect(websocket, user_id)
    
    #Send connection confirmation
    await websocket.send_json({
        "type": "connected",
        "data": {
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat(),
            "message": "WebSocket 连接成功"
        }
    })
    
    try:
        #Heart beat.
        async def send_heartbeat():
            while True:
                try:
                    await asyncio.sleep(30)  #Send a heart every 30 seconds Jump!
                    await websocket.send_json({
                        "type": "heartbeat",
                        "data": {
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    })
                except Exception as e:
                    logger.debug(f"[WS] Heart beating failed:{e}")
                    break
        
        #Start a heartbeat.
        heartbeat_task = asyncio.create_task(send_heartbeat())
        
        #Receive client messages (mainly for maintaining connections)
        while True:
            try:
                data = await websocket.receive_text()
                #Can process messages sent by client (e.g. ping/pong)
                logger.debug(f"[WS]{user_id}, data={data}")
            except WebSocketDisconnect:
                logger.info(f"[WS] Client actively disconnected: user={user_id}")
                break
            except Exception as e:
                logger.error(f"[WS] Message reception error:{e}")
                break
    
    finally:
        #Cancel the heartbeat.
        if 'heartbeat_task' in locals():
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
        
        #Disconnect
        await manager.disconnect(websocket, user_id)


@router.websocket("/ws/tasks/{task_id}")
async def websocket_task_progress_endpoint(
    websocket: WebSocket,
    task_id: str,
    token: str = Query(...)
):
    """WebSocket task progressend Points

    Client connection: ws://localhost: 8,000/api/ws/tasks/<task id>?token=<jwt token>

    Message format:
    FMT 0 
    ♪ I'm sorry ♪
    """
    #Authentication token
    token_data = AuthService.verify_token(token)
    if not token_data:
        await websocket.close(code=1008, reason="Unauthorized")
        return
    
    user_id = "admin"
    channel = f"task_progress:{task_id}"
    
    #Connect WebSocket
    await websocket.accept()
    logger.info(f"[WS-Task]{task_id}, user={user_id}")
    
    #Send connection confirmation
    await websocket.send_json({
        "type": "connected",
        "data": {
            "task_id": task_id,
            "timestamp": datetime.utcnow().isoformat(),
            "message": "已连接任务进度流"
        }
    })
    
    try:
        #Here you can get task progress from the Redis or database
        #Stay connected until the mission is completed
        while True:
            try:
                data = await websocket.receive_text()
                logger.debug(f"[WS-Task]{task_id}, data={data}")
            except WebSocketDisconnect:
                logger.info(f"[WS-Task] Client voluntarily disconnected:{task_id}")
                break
            except Exception as e:
                logger.error(f"[WS-Task]{e}")
                break
    
    finally:
        logger.info(f"[WS-Task] Disconnected:{task_id}")


@router.get("/ws/stats")
async def get_websocket_stats():
    """Get WebSocket Connection Statistics"""
    return manager.get_stats()


#🔥 support function: for other modules to call, send notifications
async def send_notification_via_websocket(user_id: str, notification: dict):
    """Send notification via WebSocket

    Args:
        user id: userID
        Notification data
    """
    message = {
        "type": "notification",
        "data": notification
    }
    await manager.send_personal_message(message, user_id)


async def send_task_progress_via_websocket(task_id: str, progress_data: dict):
    """Send task progress via WebSocket

    Args:
        task id: task ID
        progress data: progress data
    """
    #Note: We need to know which user the task belongs to.
    #Sendable from database query or from project data
    #Simplified processing temporarily
    message = {
        "type": "progress",
        "data": progress_data
    }
    #Broadcast to all connections (production environment should be distributed only to mission-owned users)
    await manager.broadcast(message)

