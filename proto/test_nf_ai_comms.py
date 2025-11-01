import unittest
import time

# Import the generated message classes
from nf_ai_comms_pb2 import TaskObservation, Action

class TestNfAiCommsMessages(unittest.TestCase):

    def test_task_observation_creation_and_fields(self):
        """Test creating a TaskObservation message and accessing its fields."""
        event_id = "obs_123"
        event_type = "task_start"
        timestamp = time.time() # Using float timestamp for simplicity in test
        iso_timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(timestamp))
        pipeline_name = "test_pipeline"
        process_name = "test_process"
        task_id_num = 1
        task_hash = "abcdef123456"
        task_name = "test_process (1)"
        native_id = "native_001"
        status = "RUNNING"

        # Create a TaskObservation instance
        observation = TaskObservation(
            event_id=event_id,
            event_type=event_type,
            timestamp_iso=iso_timestamp,
            pipeline_name=pipeline_name,
            process_name=process_name,
            task_id_num=task_id_num,
            task_hash=task_hash,
            task_name=task_name,
            native_id=native_id,
            status=status
            # We can skip completion-specific fields for a 'task_start' event
        )

        # Assertions to check if fields are set correctly
        self.assertEqual(observation.event_id, event_id)
        self.assertEqual(observation.event_type, event_type)
        self.assertEqual(observation.timestamp_iso, iso_timestamp)
        self.assertEqual(observation.pipeline_name, pipeline_name)
        self.assertEqual(observation.process_name, process_name)
        self.assertEqual(observation.task_id_num, task_id_num)
        self.assertEqual(observation.task_hash, task_hash)
        self.assertEqual(observation.task_name, task_name)
        self.assertEqual(observation.native_id, native_id)
        self.assertEqual(observation.status, status)

        # Test default values for unset fields (e.g., exit_code should be 0 for int32)
        self.assertEqual(observation.exit_code, 0)
        self.assertEqual(observation.duration_ms, 0)

    def test_action_creation_and_fields(self):
        """Test creating an Action message and accessing its fields."""
        observation_event_id = "obs_123"
        action_id = "act_789"
        action_details = "echo_received_and_processed"
        success = True
        message = "Observation processed successfully."

        # Create an Action instance
        action = Action(
            observation_event_id=observation_event_id,
            action_id=action_id,
            action_details=action_details,
            success=success,
            message=message
        )

        # Assertions
        self.assertEqual(action.observation_event_id, observation_event_id)
        self.assertEqual(action.action_id, action_id)
        self.assertEqual(action.action_details, action_details)
        self.assertEqual(action.success, success)
        self.assertEqual(action.message, message)

    def test_task_observation_serialization_deserialization(self):
        """Test serializing and deserializing a TaskObservation message."""
        observation = TaskObservation(
            event_id="obs_serialize_test",
            event_type="task_complete",
            timestamp_iso=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            pipeline_name="serialize_pipeline",
            process_name="serialize_process",
            task_id_num=10,
            exit_code=0,
            duration_ms=1000
        )

        # Serialize to string
        serialized_data = observation.SerializeToString()
        self.assertIsInstance(serialized_data, bytes)

        # Deserialize back to an object
        new_observation = TaskObservation()
        new_observation.ParseFromString(serialized_data)

        # Check if the deserialized object matches the original
        self.assertEqual(new_observation.event_id, "obs_serialize_test")
        self.assertEqual(new_observation.event_type, "task_complete")
        self.assertEqual(new_observation.pipeline_name, "serialize_pipeline")
        self.assertEqual(new_observation.task_id_num, 10)
        self.assertEqual(new_observation.duration_ms, 1000)

if __name__ == '__main__':
    unittest.main()
