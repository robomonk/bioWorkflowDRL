import grpc
from concurrent import futures
import time

# Import the generated classes
import sys
sys.path.append('../proto')  # Add proto folder to Python path
import dummy_pb2
import dummy_pb2_grpc

# Create a class to define the server functions, derived from
# dummy_pb2_grpc.GreeterServicer
class GreeterServicer(dummy_pb2_grpc.GreeterServicer):
    # Implement the SayHello RPC
    def SayHello(self, request, context):
        response = dummy_pb2.HelloReply()
        response.message = f"Hello, {request.name}!"
        return response

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Add the Greeter service
    dummy_pb2_grpc.add_GreeterServicer_to_server(GreeterServicer(), server)

    # Listen on port 50051
    print("Starting server. Listening on port 50051.")
    server.add_insecure_port('[::]:50051')
    server.start()

    # Keep the server running
    try:
        while True:
            time.sleep(86400)  # One day in seconds
    except KeyboardInterrupt:
        server.stop(0)
        print("Server stopped.")

if __name__ == '__main__':
    serve()
