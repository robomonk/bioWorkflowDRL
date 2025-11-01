import grpc
import time
import asyncio
from concurrent import futures
import uuid # For generating unique action IDs

import ray

# Import the generated gRPC files
try:
    # This assumes PYTHONPATH is set to BioWorkFlowML or you run from BioWorkFlowML
    from proto import nf_ai_comms_pb2
    from proto import nf_ai_comms_pb2_grpc
except ImportError:
    import sys
    import os

    # Get the absolute path to the directory containing the current script (ai_action_streamer)
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    # Get the project root directory (BioWorkFlowML)
    project_root_dir = os.path.dirname(current_script_dir)
    # Get the path to the 'proto' directory
    proto_dir = os.path.join(project_root_dir, 'proto')

    # Add the project root to sys.path to allow 'from proto import ...'
    if project_root_dir not in sys.path:
        sys.path.insert(0, project_root_dir)
    
    if proto_dir not in sys.path:
        sys.path.insert(0, proto_dir)

    import nf_ai_comms_pb2
    import nf_ai_comms_pb2_grpc


# Define the servicer class that implements the RPC methods
class AiActionServicer(nf_ai_comms_pb2_grpc.AiActionServiceServicer):
    async def SendTaskObservation(self, request: nf_ai_comms_pb2.TaskObservation, context):
        print(f"AiActionStreamer: Received observation_event_id: {request.event_id}, type: {request.event_type}")
        print(f"  Pipeline: {request.pipeline_name}, Process: {request.process_name}, Task: {request.task_name}")

        await asyncio.sleep(0.01) 

        action_id = f"act_{uuid.uuid4()}"
        response_message = f"AiActionStreamer: Echoed observation_event_id {request.event_id}"
        print(f"  Sending action_id: {action_id}")

        return nf_ai_comms_pb2.Action(
            observation_event_id=request.event_id, 
            action_id=action_id,
            action_details="echo_received_and_processed", 
            success=True,
            message=response_message
        )

@ray.remote
class AiActionStreamer:
    # Make the __init__ method asynchronous
    async def __init__(self, host="[::]", port=50051):
        self.host = host
        self.port = port
        self.server = None
        print(f"AiActionStreamer Actor initialized. Will listen on {self.host}:{self.port}")

    async def start_server(self):
        self.server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        nf_ai_comms_pb2_grpc.add_AiActionServiceServicer_to_server(
            AiActionServicer(), self.server
        )
        self.server.add_insecure_port(f"{self.host}:{self.port}")
        await self.server.start()
        print(f"AiActionStreamer gRPC server started on {self.host}:{self.port}")
        try:
            await self.server.wait_for_termination()
        except KeyboardInterrupt:
            print("AiActionStreamer server shutting down due to KeyboardInterrupt...")
        except Exception as e:
            print(f"AiActionStreamer server error: {e}")
        finally:
            await self.stop_server()


    async def stop_server(self):
        if self.server:
            print("Stopping AiActionStreamer gRPC server...")
            await self.server.stop(grace=1.0) 
            self.server = None
            print("AiActionStreamer gRPC server stopped.")

    def get_port(self): 
        return self.port

async def main_server_loop():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True, log_to_driver=False)

    broker_port = 50051 
    ai_streamer_actor = AiActionStreamer.options(name="AiActionStreamerService", get_if_exists=True).remote(port=broker_port)

    print("Attempting to start AiActionStreamer server via Ray actor...")
    server_task_future = ai_streamer_actor.start_server.remote()
    # Ensure the actor has time to initialize before getting the port
    # A small delay or a more robust check might be needed in some cases
    await asyncio.sleep(0.1) 
    print(f"AiActionStreamer server launch initiated. Expected to listen on port {await ai_streamer_actor.get_port.remote()}")

    try:
        while True:
            await asyncio.sleep(3600) 
    except KeyboardInterrupt:
        print("Application shutting down by KeyboardInterrupt...")
    except Exception as e:
        print(f"Main loop encountered an error: {e}")
    finally:
        print("Ensuring AiActionStreamer server is stopped...")
        await ai_streamer_actor.stop_server.remote()
        if ray.is_initialized():
            ray.shutdown()
        print("Ray shut down. Exiting.")


if __name__ == "__main__":
    try:
        asyncio.run(main_server_loop())
    except KeyboardInterrupt:
        print("Exiting main application script...")
