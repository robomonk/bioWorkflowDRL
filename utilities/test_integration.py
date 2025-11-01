import sys
import os
import datetime
import uuid
import time

# Add project root to sys.path to allow utilities.ai_server and utilities.nf_client imports
# This is needed when running 'python utilities/test_integration.py' from the project root,
# or if 'utilities' is not directly in PYTHONPATH.
# Assumes this script is in 'utilities' and project root is one level up.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
proto_dir = os.path.join(project_root, 'proto')

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if proto_dir not in sys.path:
    sys.path.insert(0, proto_dir) # Add proto dir directly

import grpc # For grpc.RpcError and grpc.FutureTimeoutError (though FutureTimeoutError is part of RpcError)

# Now that sys.path is adjusted, we can import from utilities and proto deps should be found
from utilities.ai_server import AiServer
from utilities.nf_client import send_task_observation


TEST_SERVER_PORT = 50059
TEST_LOG_FILE = "/tmp/test_ai_server.log"

if __name__ == "__main__":
    print("Starting integration test for asynchronous client call...")

    # Instantiate and start the AI Server
    ai_server = AiServer(port=TEST_SERVER_PORT, log_file=TEST_LOG_FILE)
    print(f"Attempting to start AI Server on port {TEST_SERVER_PORT} with log file {TEST_LOG_FILE}...")
    ai_server.start()
    print("AI Server started.")

    # Allow server a moment to fully start
    time.sleep(2)

    # Prepare sample observation data
    sample_event_id = str(uuid.uuid4())
    sample_observation_data = {
        "event_id": sample_event_id,
        "event_type": "test_event_from_async_integration_script",
        "timestamp_iso": datetime.datetime.utcnow().isoformat() + "Z",
        "pipeline_name": "async_integration_test_pipeline",
        "process_name": "async_integration_test_process",
        "task_id_num": "888",
        "status": "TESTING_ASYNC",
    }

    print(f"Calling send_task_observation (event_id={sample_event_id}) to localhost:{TEST_SERVER_PORT}...")
    response_future = None
    test_success = False
    try:
        response_future = send_task_observation(
            sample_observation_data,
            server_address=f'localhost:{TEST_SERVER_PORT}'
        )

        print("Future object received. Waiting for result with timeout (10s)...")
        # Block on the future to get the result for assertions.
        action_response = response_future.result(timeout=10)

        assert action_response is not None, "Client did not receive a response (action_response is None)."

        print(f"Received action: ID={action_response.action_id}, Success={action_response.success}")
        print(f"Details: {action_response.action_details}")
        print(f"Correlated Event ID: {action_response.observation_event_id}")

        assert action_response.success, "Action response success was not True!"
        assert action_response.observation_event_id == sample_event_id, \
            f"Observation event ID mismatch! Expected {sample_event_id}, Got {action_response.observation_event_id}"

        print("Client received valid and successful response from future.")
        test_success = True

        if test_success: # Should be true if assertions passed
            print("Integration test successful!")
        else:
            # This path should ideally not be hit if assertions raise errors,
            # but kept for logical completeness if an assertion failure didn't stop execution.
            print("Integration test failed: Assertions did not pass or response was invalid.")

    except grpc.FutureTimeoutError:
        print("Integration test failed: Timeout waiting for future result.")
        test_success = False
    except grpc.RpcError as e:
        print(f"Integration test failed: RPC error: {e.code()} - {e.details()}")
        test_success = False
    except AssertionError as ae:
        print(f"Integration test failed: Assertion Error: {ae}")
        test_success = False
    except Exception as e:
        print(f"Integration test failed with unexpected exception: {type(e).__name__} - {e}")
        test_success = False

    finally:
        print("Stopping AI Server...")
        ai_server.stop(0) # Grace period 0 for immediate stop
        print("AI Server stopped.")

        # Log inspection (optional, but useful for debugging)
        if os.path.exists(TEST_LOG_FILE):
            print(f"\n--- Content of {TEST_LOG_FILE} ---")
            with open(TEST_LOG_FILE, "r") as f:
                print(f.read())
            print("--- End of log ---")
            # For automated tests, you might choose to remove the log file:
            # os.remove(TEST_LOG_FILE)
        else:
            print(f"Test log file {TEST_LOG_FILE} was not created.")

    # Exit with a status code reflecting test success or failure
    if not test_success:
        sys.exit(1) # Indicate failure
    else:
        sys.exit(0) # Indicate success
