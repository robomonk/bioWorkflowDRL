import grpc

# Import the generated classes
import sys
sys.path.append('../proto')  # Add proto folder to Python path
import dummy_pb2
import dummy_pb2_grpc

def run():
    # Connect to the server
    channel = grpc.insecure_channel('localhost:50051')

    # Create a stub (client)
    stub = dummy_pb2_grpc.GreeterStub(channel)

    # Create a request message
    request = dummy_pb2.HelloRequest()
    request.name = "World"

    # Make the call
    try:
        response = stub.SayHello(request)
        print(f"Greeter client received: {response.message}")
    except grpc.RpcError as e:
        print(f"Error: {e.details()} (code: {e.code()})")
    finally:
        channel.close()

if __name__ == '__main__':
    run()
