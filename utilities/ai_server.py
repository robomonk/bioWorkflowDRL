import grpc
from concurrent import futures
import time
import uuid
import grpc
from concurrent import futures

# Import the generated classes
# Assuming 'proto' directory is in PYTHONPATH or handled by the calling script.
import nf_ai_comms_pb2
import nf_ai_comms_pb2_grpc

# AiActionServiceServicer remains largely the same but uses a passed-in logger
class AiActionServiceServicer(nf_ai_comms_pb2_grpc.AiActionServiceServicer):
    def __init__(self, logger_callable):
        self.logger = logger_callable

    def SendTaskObservation(self, request, context):
        self.logger(f"Received TaskObservation: event_id={request.event_id}, event_type={request.event_type}")
        response = nf_ai_comms_pb2.Action()
        response.observation_event_id = request.event_id
        response.action_id = str(uuid.uuid4())
        response.action_details = f"Action for event {request.event_id}: Processed event type '{request.event_type}'"
        response.success = True
        response.message = "Successfully processed TaskObservation"
        self.logger(f"Sending Action: action_id={response.action_id}")
        return response

class AiServer:
    def __init__(self, port=50052, log_file="/tmp/ai_server.log"):
        self.port = port
        self.log_file = log_file
        self.server = None

    def app_log(self, message):
        with open(self.log_file, "a") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

    def start(self):
        # Initialize logging (clear/create log file)
        with open(self.log_file, "w") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Log initialized for AiServer.\n")

        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

        # Instantiate servicer with the app_log method
        servicer = AiActionServiceServicer(self.app_log)
        nf_ai_comms_pb2_grpc.add_AiActionServiceServicer_to_server(servicer, self.server)

        self.server.add_insecure_port(f'[::]:{self.port}')
        self.server.start()
        self.app_log(f"AiServer started. Listening on port {self.port}.")

    def stop(self, grace=None):
        self.app_log("AiServer stopping.")
        if self.server:
            self.server.stop(grace)
        self.app_log("AiServer stopped.")

    def wait_for_termination(self):
        if self.server:
            self.server.wait_for_termination()

# Updated main execution block
if __name__ == '__main__':
    ai_server = AiServer() # Uses default port and log file
    ai_server.start()
    print(f"AiServer running on port {ai_server.port}. Press Ctrl+C to stop.")
    try:
        ai_server.wait_for_termination()
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Stopping server...")
        ai_server.stop(0) # Grace period 0 for immediate stop
    print("Server shut down gracefully.")
