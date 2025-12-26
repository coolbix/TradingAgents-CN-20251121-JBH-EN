"""WebSocket Connection Manager
For real-time transmission of progress updates
"""

import asyncio
import json
import logging
from typing import Dict, Set, Any
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

class WebSocketManager:
    """WebSocket Connection Manager"""
    
    def __init__(self):
        #Store active connection:   FT 0}
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket, task_id: str):
        """Create WebSocket Connection"""
        await websocket.accept()
        
        async with self._lock:
            if task_id not in self.active_connections:
                self.active_connections[task_id] = set()
            self.active_connections[task_id].add(websocket)
        
        logger.info(f"WebSocket links to:{task_id}")
    
    async def disconnect(self, websocket: WebSocket, task_id: str):
        """Disconnect WebSocket Connection"""
        async with self._lock:
            if task_id in self.active_connections:
                self.active_connections[task_id].discard(websocket)
                if not self.active_connections[task_id]:
                    del self.active_connections[task_id]
        
        logger.info(f"WebSocket is disconnected:{task_id}")
    
    async def send_progress_update(self, task_id: str, message: Dict[str, Any]):
        """Send progress updates to all connections of the given task"""
        if task_id not in self.active_connections:
            return
        
        #Copy the connection set to avoid changes during an iterative period
        connections = self.active_connections[task_id].copy()
        
        for connection in connections:
            try:
                await connection.send_text(json.dumps(message))
            except Exception as e:
                logger.warning(f"Could not close temporary folder: %s{e}")
                #Remove invalid connection
                async with self._lock:
                    if task_id in self.active_connections:
                        self.active_connections[task_id].discard(connection)
    
    async def broadcast_to_user(self, user_id: str, message: Dict[str, Any]):
        """Broadcast all connections to users"""
        #This can be expanded to manage connections by user ID.
        #Currently streamlined and achieved, managed only by task ID
        pass
    
    async def get_connection_count(self, task_id: str) -> int:
        """Get the number of connections for specified tasks"""
        async with self._lock:
            return len(self.active_connections.get(task_id, set()))
    
    async def get_total_connections(self) -> int:
        """Get total connections"""
        async with self._lock:
            total = 0
            for connections in self.active_connections.values():
                total += len(connections)
            return total

#Global Examples
_websocket_manager = None

def get_websocket_manager() -> WebSocketManager:
    """Get instance of a WebSocket manager"""
    global _websocket_manager
    if _websocket_manager is None:
        _websocket_manager = WebSocketManager()
    return _websocket_manager
