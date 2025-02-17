from dataclasses import dataclass
from typing import Dict, Set, Any, Optional
from enum import Enum
import asyncio
import json
import websockets
from datetime import datetime


class PipelineStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass
class PipelineInfo:
    """Data structure to hold pipeline information"""
    pipeline_id: str
    name: str
    status: PipelineStatus
    last_update: datetime
    metadata: dict
    error_message: Optional[str] = None


class PipelineDashboard:
    """
    Dashboard that monitors multiple pipeline instances using WebSocket communication.
    """

    def __init__(self, host: str = "localhost", port: int = 8765):
        self.logger = logging.getLogger(__name__)
        self.host = host
        self.port = port
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        # Store information about all pipelines
        self.pipelines: Dict[str, PipelineInfo] = {}
        # Aggregate statistics
        self.global_stats: Dict[str, Any] = {
            'total_pipelines': 0,
            'active_pipelines': 0,
            'failed_pipelines': 0,
            'completed_pipelines': 0,
            'idle_pipelines': 0
        }

    async def update_pipeline_status(self, pipeline_data: dict) -> None:
        """
        Update status and information for a specific pipeline.
        """
        pipeline_id = pipeline_data['pipeline_id']
        status = PipelineStatus(pipeline_data['status'])

        if pipeline_id not in self.pipelines:
            # New pipeline registration
            self.pipelines[pipeline_id] = PipelineInfo(
                pipeline_id=pipeline_id,
                name=pipeline_data.get('name', f'Pipeline-{pipeline_id}'),
                status=status,
                last_update=datetime.now(),
                metadata=pipeline_data.get('metadata', {})
            )
        else:
            # Update existing pipeline
            self.pipelines[pipeline_id].status = status
            self.pipelines[pipeline_id].last_update = datetime.now()
            self.pipelines[pipeline_id].metadata.update(pipeline_data.get('metadata', {}))

            if 'error_message' in pipeline_data:
                self.pipelines[pipeline_id].error_message = pipeline_data['error_message']

        await self.update_global_stats()
        await self.broadcast_updates()

    async def update_global_stats(self) -> None:
        """
        Update global statistics based on all pipeline states.
        """
        stats = {status: 0 for status in PipelineStatus}
        for pipeline in self.pipelines.values():
            stats[pipeline.status] += 1

        self.global_stats.update({
            'total_pipelines': len(self.pipelines),
            'active_pipelines': stats[PipelineStatus.RUNNING],
            'failed_pipelines': stats[PipelineStatus.FAILED],
            'completed_pipelines': stats[PipelineStatus.COMPLETED],
            'idle_pipelines': stats[PipelineStatus.IDLE]
        })

    async def broadcast_updates(self) -> None:
        """
        Broadcast updated pipeline information to all connected clients.
        """
        if not self.clients:
            return

        message = {
            'type': 'dashboard_update',
            'timestamp': datetime.now().isoformat(),
            'global_stats': self.global_stats,
            'pipelines': {
                pid: {
                    'name': p.name,
                    'status': p.status.value,
                    'last_update': p.last_update.isoformat(),
                    'metadata': p.metadata,
                    'error_message': p.error_message
                } for pid, p in self.pipelines.items()
            }
        }

        await asyncio.gather(
            *(client.send(json.dumps(message)) for client in self.clients)
        )

    async def handle_message(self, websocket: websockets.WebSocketServerProtocol, message: str) -> None:
        """
        Handle incoming messages from pipeline scripts.
        """
        try:
            data = json.loads(message)
            message_type = data.get('type', '')

            if message_type == 'pipeline_update':
                await self.update_pipeline_status(data['payload'])
            elif message_type == 'pipeline_heartbeat':
                await self.handle_heartbeat(data['payload'])
            elif message_type == 'get_pipeline_status':
                await self.send_pipeline_status(websocket, data['payload']['pipeline_id'])
        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
            await self.handle_error(websocket, str(e))

    async def monitor_pipeline_timeouts(self) -> None:
        """
        Monitor pipelines for timeouts and inactivity.
        """
        while True:
            current_time = datetime.now()
            for pipeline_id, pipeline in list(self.pipelines.items()):
                time_diff = (current_time - pipeline.last_update).total_seconds()

                # Mark pipeline as failed if no update received for 5 minutes
                if time_diff > 300 and pipeline.status == PipelineStatus.RUNNING:
                    pipeline.status = PipelineStatus.FAILED
                    pipeline.error_message = "Pipeline timeout - no updates received"
                    await self.update_global_stats()
                    await self.broadcast_updates()

            await asyncio.sleep(60)  # Check every minute

    async def run_server(self) -> None:
        """
        Start the WebSocket server and monitoring tasks.
        """
        self.logger.info(f"Starting Dashboard WebSocket server on {self.host}:{self.port}")
        async with websockets.serve(self.handle_client, self.host, self.port):
            # Start the pipeline monitoring task
            monitor_task = asyncio.create_task(self.monitor_pipeline_timeouts())
            try:
                await asyncio.Future()  # run forever
            finally:
                monitor_task.cancel()