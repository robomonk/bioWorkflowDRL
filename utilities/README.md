# Utilities: gRPC Server and Client for AI Comms

This directory contains a gRPC server (`ai_server.py`) and a client (`nf_client.py`) designed for communication between a Nextflow plugin and AI actors, using definitions from `proto/nf_ai_comms.proto`.

## `ai_server.py` (AI Actor Usage)

The `ai_server.py` implements the `AiActionService` gRPC service. It's designed to be run by an AI actor (e.g., a Ray actor) to listen for `TaskObservation` messages from a Nextflow process (via `nf_client.py`) and respond with `Action` messages.

### Dependencies
- `grpcio`
- `grpcio-tools` (for protobuf compilation, not strictly a runtime dep for the server itself if pb2 files are present)
- Python 3.x

### How to Use from an AI Actor (e.g., Ray Actor)

1.  **Import the `AiServer` class:**
    ```python
    from utilities.ai_server import AiServer
    ```

2.  **Instantiate the server:**
    You can specify the port and log file path.
    ```python
    # Default port is 50052, default log file is /tmp/ai_server.log
    server_instance = AiServer(port=50052, log_file="/path/to/your/ai_server.log")
    ```

3.  **Start the server:**
    This is a non-blocking call; it starts the gRPC server in a separate thread pool.
    ```python
    server_instance.start()
    print(f"AI Server started on port {server_instance.port}")
    ```

4.  **Keep the actor alive / Wait for termination:**
    The gRPC server runs in background threads. Your main actor process needs to be kept alive. You can use `server_instance.wait_for_termination()` if you want the current thread to block until the server is stopped.
    ```python
    # To block the current thread until server stops (e.g., by KeyboardInterrupt or programmatically)
    # try:
    #     server_instance.wait_for_termination()
    # except KeyboardInterrupt:
    #     print("Shutting down server...")
    # finally:
    #     server_instance.stop(0) # 0 is grace period in seconds

    # Alternatively, if your Ray actor has its own lifecycle,
    # ensure you call stop() when the actor is terminated.
    ```

5.  **Stop the server:**
    Call this method to gracefully shut down the server.
    ```python
    server_instance.stop(grace=0) # grace is the period in seconds to allow ongoing RPCs to complete.
    print("AI Server stopped.")
    ```

### Server Behavior
-   Listens for `TaskObservation` messages.
-   For each observation, it logs the reception, processes it (currently, it creates a generic `Action` response), and sends the `Action` back.
-   Logs its activities to the specified log file (default: `/tmp/ai_server.log`).

### Protocol
-   Adheres to the service and message definitions in `proto/nf_ai_comms.proto`.

## `nf_client.py` (Nextflow Plugin Usage)

The `nf_client.py` provides a function to send `TaskObservation` messages to the `ai_server.py` and receive an `Action` response. It's intended to be used by a Nextflow plugin.

### Dependencies
- `grpcio`
- `grpcio-tools` (for protobuf compilation, not strictly a runtime dep for the client itself if pb2 files are present)
- Python 3.x
- `uuid` (for default `event_id` generation if not provided)
- `datetime` (for default `timestamp_iso` generation if not provided)

### How to Use from a Nextflow Plugin

1.  **Import the `send_task_observation` function:**
    ```python
    from utilities.nf_client import send_task_observation
    # Ensure that the `proto` directory is also in the Python path if nf_ai_comms_pb2 is not installed globally.
    # This might require adding the path to sys.path in your Nextflow plugin script:
    # import sys
    # sys.path.append('/path/to/your/project/proto')
    # sys.path.append('/path/to/your/project') # To find utilities.nf_client
    ```

2.  **Prepare `TaskObservation` data:**
    This should be a Python dictionary. The keys should correspond to the fields in the `TaskObservation` message defined in `proto/nf_ai_comms.proto`.
    ```python
    import datetime
    import uuid

    observation_data = {
        "event_id": str(uuid.uuid4()),
        "event_type": "task_complete",
        "timestamp_iso": datetime.datetime.utcnow().isoformat() + "Z",
        "pipeline_name": "my_pipeline",
        "process_name": "my_process",
        "task_id_num": 123,
        "task_hash": "xxxyyyzzz",
        "task_name": "my_process (1)",
        "native_id": "slurm_12345",
        "status": "COMPLETED",
        "exit_code": 0,
        "duration_ms": 50000,
        "realtime_ms": 52000,
        "cpu_percent": "150.0%", # As string
        "peak_rss_bytes": 1024 * 1024 * 200, # 200 MB
        "peak_vmem_bytes": 1024 * 1024 * 500, # 500 MB
        "read_bytes": 1024 * 10, # 10 KB
        "write_bytes": 1024 * 5 # 5 KB
        # Add other fields as necessary
    }
    ```
    *Note: The `send_task_observation` function provides default values for `event_id` (a new UUID) and `event_type` ("") if they are not present in the dictionary. It also attempts to convert numeric types, but it's best to provide them in the correct format (e.g., integers for byte counts, strings for percentages like "cpu_percent").*


3.  **Call the function (Asynchronously):**
    The `send_task_observation` function now returns a `grpc.Future` object. This allows for non-blocking calls.
    ```python
    ai_server_address = 'localhost:50052' # Or your configured AI server address
    future = send_task_observation(observation_data, server_address=ai_server_address)
    print(f"Task observation sent, future received: {future}")
    ```

4.  **Process the response (Handling the Future):**
    You need to handle the `grpc.Future` object to get the actual `Action` response. This is typically done in one of two ways:

    *   **a) Using `future.result()` (Blocking for this specific future):**
        You can wait for a specific future to complete and get its result. This is useful if you need the response before proceeding with a particular piece of logic.
        ```python
        import grpc # Required for exception handling

        try:
            # Wait for the result, with an optional timeout (in seconds)
            action_response = future.result(timeout=10)
            if action_response:
                print(f"Received action: ID={action_response.action_id}, Success={action_response.success}")
                # ... use the action_response fields as needed ...
            else:
                # This case might not happen if future.result() raises an exception on error
                print("Received no action response (should have raised exception on error).")
        except grpc.FutureTimeoutError:
            print(f"Timeout waiting for action response for event {observation_data.get('event_id')}")
        except grpc.RpcError as e:
            print(f"RPC error for event {observation_data.get('event_id')}: {e.details()} (code: {e.code()})")
        except Exception as e:
            print(f"An unexpected error occurred processing future for event {observation_data.get('event_id')}: {e}")
        ```

    *   **b) Using a callback function (Non-blocking):**
        This is a more common pattern for fully asynchronous workflows. The callback function will be invoked when the RPC completes.
        ```python
        import grpc # Required for exception types if checked in callback

        def my_callback(future_obj):
            try:
                action_response = future_obj.result() # Get the result
                if action_response:
                    print(f"[Callback] Received action: ID={action_response.action_id}, Success={action_response.success}")
                    # ... process action_response ...
                else:
                    print("[Callback] No action response received.")
            except grpc.RpcError as e:
                print(f"[Callback] RPC error: {e.details()}")
            except Exception as e:
                print(f"[Callback] Error in callback: {e}")

        future.add_done_callback(my_callback)
        # The main program can continue doing other things here.
        # Ensure the main program stays alive long enough for callbacks to fire.
        ```

### Return Value
-   The function returns a `grpc.Future` object. The actual `nf_ai_comms_pb2.Action` protobuf message is obtained by calling `result()` on this future, typically within a callback or a try-except block.

### Important Note on Channel Management
-   The current `send_task_observation` function creates a new gRPC channel for each call but **does not close it**. In a high-throughput scenario where many observations are sent, this could lead to resource leakage (e.g., too many open file descriptors).
-   For production use in a Nextflow plugin that sends many observations, consider implementing a more robust channel management strategy:
    -   Create a single channel when the plugin initializes.
    -   Pass this channel to `send_task_observation` (this would require modifying the function to accept an optional channel argument).
    -   Close the channel when the plugin shuts down.
    -   Alternatively, the `nf_client.py` module could manage a global channel.
-   This aspect may be refined in future versions of `nf_client.py`.

### Protocol
-   Adheres to the service and message definitions in `proto/nf_ai_comms.proto`.
