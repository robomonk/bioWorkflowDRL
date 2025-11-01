import grpc
import asyncio
import uuid # For event_id

# Assuming your generated proto files are in a 'proto' subdirectory
# relative to where you run this client, or BioWorkFlowML is in PYTHONPATH.
try:
    from proto import nf_ai_comms_pb2
    from proto import nf_ai_comms_pb2_grpc
except ImportError:
    # Fallback if running directly and proto is a sibling or needs path adjustment
    import sys
    import os
    # Adjust this path if your 'proto' directory is located elsewhere relative to this script
    sys.path.append(os.path.join(os.path.dirname(__file__), '.')) # Current dir for proto
    # Or if proto is in parent and this script is in a subdir:
    # sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from proto import nf_ai_comms_pb2
    from proto import nf_ai_comms_pb2_grpc


async def run_client(server_address="localhost:50051"):
    """
    Connects to the AiActionStreamer server, sends a sample TaskObservation,
    and prints the received Action.
    """
    print(f"Attempting to connect to server at {server_address}...")
    try:
        # Use grpc.aio.insecure_channel for an asyncio client
        async with grpc.aio.insecure_channel(server_address) as channel:
            print("Successfully connected to server.")
            stub = nf_ai_comms_pb2_grpc.AiActionServiceStub(channel)

            # Create a sample TaskObservation message
            event_id = f"obs_client_test_{uuid.uuid4()}"
            sample_observation = nf_ai_comms_pb2.TaskObservation(
                event_id=event_id,
                event_type="task_start_client_test",
                timestamp_iso="2023-10-27T10:00:00Z",
                pipeline_name="client_test_pipeline",
                process_name="client_test_process",
                task_id_num=123,
                task_hash="clienthash789",
                task_name="client_test_process (1)",
                native_id="client_native_007",
                status="RUNNING_FROM_CLIENT"
            )

            print(f"\nSending TaskObservation to server:")
            print(f"  Event ID: {sample_observation.event_id}")
            print(f"  Event Type: {sample_observation.event_type}")
            print(f"  Pipeline: {sample_observation.pipeline_name}")

            try:
                # Call the SendTaskObservation RPC
                response_action = await stub.SendTaskObservation(sample_observation)

                print("\nReceived Action from server:")
                print(f"  Observation Event ID: {response_action.observation_event_id}")
                print(f"  Action ID: {response_action.action_id}")
                print(f"  Action Details: {response_action.action_details}")
                print(f"  Success: {response_action.success}")
                print(f"  Message: {response_action.message}")

                # Verify the echoed event ID
                if response_action.observation_event_id == event_id:
                    print("\nSUCCESS: Server correctly echoed the observation_event_id.")
                else:
                    print(f"\nERROR: Server did NOT echo the correct observation_event_id. Expected {event_id}, got {response_action.observation_event_id}")


            except grpc.aio.AioRpcError as e:
                print(f"gRPC call failed: {e.code()} - {e.details()}")

    except Exception as e:
        print(f"Failed to connect or other error: {e}")

if __name__ == "__main__":
    # Ensure you have your ai_action_streamer_server.py running in a separate terminal
    # before executing this client script.
    asyncio.run(run_client())
