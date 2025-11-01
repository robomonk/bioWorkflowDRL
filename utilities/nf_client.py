import grpc
import uuid
import datetime

# Import the generated classes
# Assuming 'proto' directory is in PYTHONPATH or handled by the calling script.
import nf_ai_comms_pb2
import nf_ai_comms_pb2_grpc

def send_task_observation(observation_data, server_address='localhost:50052'):
    """
    Sends a TaskObservation to the AiActionService asynchronously and returns a future.

    Args:
        observation_data (dict): A dictionary containing the data for the TaskObservation.
        server_address (str): The address (host:port) of the gRPC server.

    Returns:
        grpc.Future: A future object representing the asynchronous call.
                     The result of the future will be an nf_ai_comms_pb2.Action message.
                     The caller is responsible for managing the future (e.g., adding callbacks,
                     checking for exceptions, waiting for results) and for channel management
                     if making many calls (this function creates a channel per call but does not close it).
    """
    channel = grpc.insecure_channel(server_address) # Channel created per call
    stub = nf_ai_comms_pb2_grpc.AiActionServiceStub(channel)

    request = nf_ai_comms_pb2.TaskObservation()

    # Map dictionary data to protobuf message fields
    request.event_id = observation_data.get("event_id", str(uuid.uuid4()))
    request.event_type = observation_data.get("event_type", "")
    request.timestamp_iso = observation_data.get("timestamp_iso", datetime.datetime.utcnow().isoformat() + "Z")
    request.pipeline_name = observation_data.get("pipeline_name", "")
    request.process_name = observation_data.get("process_name", "")

    # Ensure numeric fields are correctly typed, providing defaults or handling missing values
    task_id_num_str = observation_data.get("task_id_num")
    if task_id_num_str is not None:
        try:
            request.task_id_num = int(task_id_num_str)
        except ValueError:
            print(f"Warning: Could not convert task_id_num '{task_id_num_str}' to int. Using default 0.")
            request.task_id_num = 0 # Default or error handling
    else:
        request.task_id_num = 0 # Default if not provided

    request.task_hash = observation_data.get("task_hash", "")
    request.task_name = observation_data.get("task_name", "")
    request.native_id = observation_data.get("native_id", "")
    request.status = observation_data.get("status", "")

    # Optional fields (example: exit_code, duration_ms)
    # Check if they exist in observation_data and set them if they do
    if "exit_code" in observation_data:
        try:
            request.exit_code = int(observation_data["exit_code"])
        except ValueError:
            print(f"Warning: Could not convert exit_code '{observation_data['exit_code']}' to int.")
            # Decide on default or leave unset if appropriate for your proto definition

    if "duration_ms" in observation_data:
        try:
            request.duration_ms = int(observation_data["duration_ms"])
        except ValueError:
            print(f"Warning: Could not convert duration_ms '{observation_data['duration_ms']}' to int.")

    if "peak_rss_bytes" in observation_data:
        try:
            request.peak_rss_bytes = int(observation_data["peak_rss_bytes"])
        except ValueError:
             print(f"Warning: Could not convert peak_rss_bytes '{observation_data['peak_rss_bytes']}' to int.")

    if "cpu_time_seconds" in observation_data:
        try:
            request.cpu_time_seconds = float(observation_data["cpu_time_seconds"])
        except ValueError:
            print(f"Warning: Could not convert cpu_time_seconds '{observation_data['cpu_time_seconds']}' to float.")

    # Add more optional fields as needed from your .proto definition
    # request.error_message = observation_data.get("error_message", "")
    # request.work_dir = observation_data.get("work_dir", "")
    # request.container_id = observation_data.get("container_id", "")
    # request.container_engine = observation_data.get("container_engine", "")
    # request.script_id = observation_data.get("script_id", "")
    # request.script_hash = observation_data.get("script_hash", "")

    # Make the non-blocking (asynchronous) call
    future = stub.SendTaskObservation.future(request)
    return future

if __name__ == '__main__':
    # This main block demonstrates how to use the asynchronous client.
    # It shows how to get the result from the future.

    # Prepare sample observation data
    sample_observation_data = {
        "event_id": str(uuid.uuid4()),
        "event_type": "task_complete_async_test",
        "timestamp_iso": datetime.datetime.utcnow().isoformat() + "Z",
        "pipeline_name": "MainAsyncPipeline",
        "process_name": "AsyncTestProcess",
        "task_id_num": "11223",
        "task_hash": "asyncfedcba",
        "task_name": "AsyncProcess (Test)",
        "native_id": "native_async_001",
        "status": "TESTING_ASYNC",
        "exit_code": "0",
        "duration_ms": "100"
    }

    print("Running nf_client standalone async test...")
    print(f"Sending async TaskObservation (event_id={sample_observation_data['event_id']})")

    # The future is returned immediately
    response_future = send_task_observation(sample_observation_data)

    print("Future object received. Waiting for result...")

    # To get the result, call result() on the future. This blocks until the RPC is complete.
    # A timeout can be provided.
    try:
        action_response = response_future.result(timeout=10) # Wait up to 10 seconds

        if action_response:
            print("\nReceived Action Details from Future:")
            print(f"  Action ID: {action_response.action_id}")
            print(f"  Correlates to Event ID: {action_response.observation_event_id}")
            print(f"  Success: {action_response.success}")
            print(f"  Message: '{action_response.message}'")
            print(f"  Details: '{action_response.action_details}'")
        else:
            # This case should ideally not be reached if result() doesn't raise an exception
            print("Failed to receive action from server (future result was None).")

    except grpc.RpcError as e:
        print(f"gRPC call failed: {e.details()} (code: {e.code()})")
    except Exception as e:
        # Catches other exceptions like timeout from future.result()
        print(f"An error occurred while waiting for future result: {e}")

    # Note: The channel used by the future is not explicitly closed here.
    # In a real application, if many calls are made, channel management is important.
    # For a single call like in this test, it's less critical as the program exits.
    print("Standalone async test finished.")
