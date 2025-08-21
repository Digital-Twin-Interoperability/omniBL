# BL Omniverse Producer - DIRECT KAFKA VERSION (Compatible with older Python)
# Reads real-time movement from Omniverse and sends directly to Kafka


import json
import logging
from datetime import datetime
from typing import Dict, Any
import time
import sys


# Hardcode the path where bl_omniverse_plugin.py lives
sys.path.append("/home/luke-cortez/dt_interoperability/hsml_api/src/firstTest")


from bl_omniverse_plugin import *
# Kafka imports
try:
   from kafka import KafkaProducer
   from kafka.errors import KafkaError
   KAFKA_AVAILABLE = True
except ImportError:
   KAFKA_AVAILABLE = False


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BLOmniverseKafkaProducer:
   # Business Logic Producer for Omniverse: reads real-time movement from Omniverse
   # and sends directly to Kafka with HSML format
  
   def __init__(self, kafka_servers=None, topic="omniverse-position-data"):
       self.stage = None
       self.omniverse_connected = False
       self.kafka_producer = None
       self.kafka_connected = False
       self.topic = topic
      
       if kafka_servers is None:
           kafka_servers = ['192.168.1.74:9092']
       self.kafka_servers = kafka_servers
      
       if not KAFKA_AVAILABLE:
           logger.error("kafka-python library not installed. Run: pip install kafka-python")
  
   def connect_to_kafka(self):
       """Connect directly to Kafka"""
       if not KAFKA_AVAILABLE:
           logger.error("Kafka library not available")
           return False
          
       try:
           logger.info("Connecting to Kafka servers: %s" % self.kafka_servers)
          
           self.kafka_producer = KafkaProducer(
               bootstrap_servers=self.kafka_servers,
               value_serializer=lambda v: json.dumps(v).encode('utf-8'),
               key_serializer=lambda k: k.encode('utf-8') if k else None,
               acks='all',  # Wait for all replicas to acknowledge
               retries=3,
               retry_backoff_ms=100,
               request_timeout_ms=30000,
               max_block_ms=10000
           )
          
           # Test the connection
           metadata = self.kafka_producer.bootstrap_connected()
           if metadata:
               self.kafka_connected = True
               logger.info("Connected to Kafka successfully")
               return True
           else:
               logger.error("Failed to connect to Kafka")
               return False
              
       except Exception as e:
           logger.error("Error connecting to Kafka: %s" % e)
           return False
  
   def connect_to_omniverse(self):
       """Connect to Omniverse and get the stage"""
       try:
           import omni.usd
           # Get the current stage
           self.stage = omni.usd.get_context().get_stage()
           if self.stage:
               self.omniverse_connected = True
               logger.info("Connected to Omniverse stage with Y-Z coordinate transformation enabled")
               return True
           else:
               logger.error("Failed to get Omniverse stage")
               return False
       except ImportError:
           logger.error("Omniverse Python API not available. Make sure you're running in Omniverse.")
           return False
       except Exception as e:
           logger.error("Error connecting to Omniverse: %s" % e)
           return False
  
   def transform_coordinates_for_output(self, position, rotation_quat):
       """
       Transform coordinates by swapping Y and Z axes before sending to Kafka
       
       Args:
           position (list): Original position [x, y, z]
           rotation_quat (list): Original rotation quaternion [x, y, z, w]
           
       Returns:
           tuple: (transformed_position, transformed_rotation)
       """
       # Transform position: swap Y and Z coordinates
       transformed_position = [
           position[0],  # X stays the same
           position[2],  # New Y = old Z
           position[1]   # New Z = old Y
       ]
       
       # Transform rotation quaternion: swap Y and Z components
       # When swapping Y and Z axes, we need to swap the Y and Z components of the quaternion
       transformed_rotation = [
           rotation_quat[0],  # X stays the same
           rotation_quat[2],  # New Y = old Z
           rotation_quat[1],  # New Z = old Y
           rotation_quat[3]   # W stays the same
       ]
       
       logger.debug("Producer coordinate transformation applied:")
       logger.debug("  Original position: x=%s, y=%s, z=%s" % (position[0], position[1], position[2]))
       logger.debug("  Transformed position: x=%s, y=%s, z=%s" % (transformed_position[0], transformed_position[1], transformed_position[2]))
       logger.debug("  Original rotation: x=%s, y=%s, z=%s, w=%s" % (rotation_quat[0], rotation_quat[1], rotation_quat[2], rotation_quat[3]))
       logger.debug("  Transformed rotation: x=%s, y=%s, z=%s, w=%s" % (transformed_rotation[0], transformed_rotation[1], transformed_rotation[2], transformed_rotation[3]))
       
       return transformed_position, transformed_rotation
  
   def test_kafka_connection(self):
       """Test Kafka connection by sending a test message"""
       if not self.kafka_connected:
           logger.error("Not connected to Kafka")
           return False
          
       try:
           logger.info("Testing Kafka connection with test message (with coordinate transformation)...")
          
           # Original test position and rotation
           original_pos = [0.0, 1.0, 2.0]  # Test values to show transformation
           original_rot = [0.0, 0.1, 0.2, 1.0]
           
           # Apply coordinate transformation
           transformed_pos, transformed_rot = self.transform_coordinates_for_output(original_pos, original_rot)
          
           # Create test message in HSML format with transformed coordinates
           test_message = {
               "entity_id": "test-omniverse-agent",
               "position": {
                   "x": transformed_pos[0],
                   "y": transformed_pos[1],
                   "z": transformed_pos[2]
               },
               "rotation": {
                   "x": transformed_rot[0],
                   "y": transformed_rot[1],
                   "z": transformed_rot[2],
                   "w": transformed_rot[3]
               },
               "timestamp": datetime.utcnow().isoformat() + "Z"
           }
          
           # Send test message
           future = self.kafka_producer.send(self.topic, test_message, key="test-key")
           record_metadata = future.get(timeout=10)  # Wait for the message to be sent
          
           logger.info("Test message with coordinate transformation sent successfully to topic '%s', partition %s" %
                      (record_metadata.topic, record_metadata.partition))
           return True
          
       except KafkaError as e:
           logger.error("Kafka test failed: %s" % e)
           return False
       except Exception as e:
           logger.error("Kafka test error: %s" % e)
           return False
  
   def send_to_kafka(self, hsml_data, key=None):
       """Send HSML data directly to Kafka"""
       if not self.kafka_connected:
           logger.error("Not connected to Kafka")
           return False
          
       try:
           if key is None:
               key = hsml_data.get('entity_id', 'unknown')
          
           logger.debug("Sending to Kafka topic '%s': %s" % (self.topic, hsml_data))
          
           # Send message to Kafka
           future = self.kafka_producer.send(self.topic, hsml_data, key=key)
          
           # Optional: wait for confirmation (adds latency but ensures delivery)
           # record_metadata = future.get(timeout=1)
           # logger.debug("Message sent to partition %s" % record_metadata.partition)
          
           return True
          
       except KafkaError as e:
           logger.error("Kafka send error: %s" % e)
           return False
       except Exception as e:
           logger.error("Error sending to Kafka: %s" % e)
           return False
  
   def extract_rotation_safely(self, transform):
       """
       Safely extract rotation from transform without using problematic quaternion constructors.
       Returns rotation as [x, y, z, w] quaternion.
       """
       try:
           # Method 1: Try ExtractRotation() and get quaternion correctly
           try:
               rotation = transform.ExtractRotation()
               quat = rotation.GetQuat()
               return [
                   quat.GetImaginary()[0],
                   quat.GetImaginary()[1],
                   quat.GetImaginary()[2],
                   quat.GetReal()
               ]
           except Exception as e1:
               logger.warning("ExtractRotation with GetQuat failed: %s" % e1)
              
               # Method 2: Try manual matrix-to-quaternion conversion
               try:
                   from pxr import Gf
                   import math
                  
                   # Extract rotation matrix
                   rotation_matrix = transform.ExtractRotationMatrix()
                  
                   # Manual matrix-to-quaternion conversion (Shepperd's method)
                   m = rotation_matrix
                   m00, m01, m02 = m[0][0], m[0][1], m[0][2]
                   m10, m11, m12 = m[1][0], m[1][1], m[1][2]
                   m20, m21, m22 = m[2][0], m[2][1], m[2][2]
                  
                   trace = m00 + m11 + m22
                  
                   if trace > 0:
                       s = math.sqrt(trace + 1.0) * 2
                       qw = 0.25 * s
                       qx = (m21 - m12) / s
                       qy = (m02 - m20) / s
                       qz = (m10 - m01) / s
                   elif m00 > m11 and m00 > m22:
                       s = math.sqrt(1.0 + m00 - m11 - m22) * 2
                       qw = (m21 - m12) / s
                       qx = 0.25 * s
                       qy = (m01 + m10) / s
                       qz = (m02 + m20) / s
                   elif m11 > m22:
                       s = math.sqrt(1.0 + m11 - m00 - m22) * 2
                       qw = (m02 - m20) / s
                       qx = (m01 + m10) / s
                       qy = 0.25 * s
                       qz = (m12 + m21) / s
                   else:
                       s = math.sqrt(1.0 + m22 - m00 - m11) * 2
                       qw = (m10 - m01) / s
                       qx = (m02 + m20) / s
                       qy = (m12 + m21) / s
                       qz = 0.25 * s
                  
                   logger.debug("Used manual matrix-to-quaternion conversion")
                   return [qx, qy, qz, qw]
                  
               except Exception as e2:
                   logger.warning("Manual matrix conversion failed: %s" % e2)
                   logger.info("Using identity quaternion as fallback")
                   return [0.0, 0.0, 0.0, 1.0]
                  
       except Exception as e:
           logger.error("All rotation extraction methods failed: %s" % e)
           return [0.0, 0.0, 0.0, 1.0]
  
   def read_movement_cube_transform(self):
       """Read transform from movement_object and convert to HSML format for Kafka"""
       object_path = "/World/CADREDemo/CADRE_Demo/Chassis"
      
       if not self.omniverse_connected:
           logger.error("Not connected to Omniverse")
           return None
          
       try:
           from pxr import UsdGeom, Gf
           # Get the prim
           prim = self.stage.GetPrimAtPath(object_path)
           if not prim.IsValid():
               logger.warning("movement_object not found at path: %s" % object_path)
               return None
          
           # Method 1: Try direct attribute access first (most reliable)
           try:
               # Initialize default values (in Omniverse coordinate system)
               translation = [0.0, 0.0, 0.0]
               rotation_quat = [0.0, 0.0, 0.0, 1.0]
              
               # Try to get translation directly from attributes
               translate_attr = prim.GetAttribute('xformOp:translate')
               if translate_attr.IsValid():
                   trans_val = translate_attr.Get()
                   if trans_val:
                       translation = [float(trans_val[0]), float(trans_val[1]), float(trans_val[2])]
                       logger.debug("Found original translation: %s" % translation)
              
               # Try to get rotation from orient attribute first
               orient_attr = prim.GetAttribute('xformOp:orient')
               if orient_attr.IsValid():
                   orient_val = orient_attr.Get()
                   if orient_val:
                       if hasattr(orient_val, 'GetReal') and hasattr(orient_val, 'GetImaginary'):
                           rotation_quat = [
                               orient_val.GetImaginary()[0],
                               orient_val.GetImaginary()[1],
                               orient_val.GetImaginary()[2],
                               orient_val.GetReal()
                           ]
                           logger.debug("Found original rotation via orient: %s" % rotation_quat)
                       else:
                           logger.warning("Orient attribute has unexpected format: %s" % type(orient_val))
               else:
                   # Try rotateXYZ attribute as fallback
                   rotate_attr = prim.GetAttribute('xformOp:rotateXYZ')
                   if rotate_attr.IsValid():
                       rot_val = rotate_attr.Get()
                       if rot_val:
                           rotation_quat = self.euler_to_quaternion(rot_val[0], rot_val[1], rot_val[2])
                           logger.debug("Found original rotation via rotateXYZ: %s" % rotation_quat)
              
               # Apply coordinate transformation before creating HSML data
               transformed_position, transformed_rotation = self.transform_coordinates_for_output(
                   translation, rotation_quat
               )
              
               # Create HSML format data with transformed coordinates
               hsml_data = {
                   "entity_id": "CADRE_Demo",
                   "position": {
                       "x": transformed_position[0],
                       "y": transformed_position[1],
                       "z": transformed_position[2]
                   },
                   "rotation": {
                       "x": transformed_rotation[0],
                       "y": transformed_rotation[1],
                       "z": transformed_rotation[2],
                       "w": transformed_rotation[3]
                   },
                   "timestamp": datetime.utcnow().isoformat() + "Z"
               }
              
               logger.debug("Read movement_object transform via direct attributes with Y-Z transformation")
               return hsml_data
              
           except Exception as attr_error:
               logger.warning("Direct attribute access failed: %s, trying matrix method" % attr_error)
              
               # Method 2: Fallback to matrix extraction
               xformable = UsdGeom.Xformable(prim)
               if not xformable:
                   logger.warning("movement_object is not transformable")
                   return None
              
               # Get the local transform matrix
               transform = xformable.GetLocalTransformation()
              
               # Extract translation and rotation (in original Omniverse coordinate system)
               translation_vec = transform.ExtractTranslation()
               rotation_quat = self.extract_rotation_safely(transform)
               
               # Convert to lists for transformation
               translation = [float(translation_vec[0]), float(translation_vec[1]), float(translation_vec[2])]
               
               # Apply coordinate transformation before creating HSML data
               transformed_position, transformed_rotation = self.transform_coordinates_for_output(
                   translation, rotation_quat
               )
              
               # Create HSML format data with transformed coordinates
               hsml_data = {
                   "entity_id": "CADRE_Demo",
                   "position": {
                       "x": transformed_position[0],
                       "y": transformed_position[1],
                       "z": transformed_position[2]
                   },
                   "rotation": {
                       "x": transformed_rotation[0],
                       "y": transformed_rotation[1],
                       "z": transformed_rotation[2],
                       "w": transformed_rotation[3]
                   },
                   "timestamp": datetime.utcnow().isoformat() + "Z"
               }
              
               logger.debug("Read movement_obj transform via matrix extraction with Y-Z transformation")
               return hsml_data
              
       except Exception as e:
           logger.error("Error reading movement_obj transform: %s" % e)
           import traceback
           logger.error(traceback.format_exc())
           return None
  
   def euler_to_quaternion(self, roll_deg, pitch_deg, yaw_deg):
       """Convert Euler angles (degrees) to quaternion [x, y, z, w]"""
       import math
      
       # Convert degrees to radians
       roll = math.radians(roll_deg)
       pitch = math.radians(pitch_deg)
       yaw = math.radians(yaw_deg)
      
       # Calculate quaternion components
       cy = math.cos(yaw * 0.5)
       sy = math.sin(yaw * 0.5)
       cp = math.cos(pitch * 0.5)
       sp = math.sin(pitch * 0.5)
       cr = math.cos(roll * 0.5)
       sr = math.sin(roll * 0.5)
      
       w = cr * cp * cy + sr * sp * sy
       x = sr * cp * cy - cr * sp * sy
       y = cr * sp * cy + sr * cp * sy
       z = cr * cp * sy - sr * sp * cy
      
       return [x, y, z, w]
  
   def send_movement_cube_data(self):
       """Read movement_obj transform and send to Kafka with coordinate transformation"""
       hsml_data = self.read_movement_cube_transform()
       if hsml_data:
           success = self.send_to_kafka(hsml_data, key="movement_obj")
           if success:
               logger.debug("Successfully sent movement_obj data to Kafka with Y-Z transformation")
               return True
           else:
               logger.error("Failed to send movement_obj data to Kafka")
               return False
       else:
           logger.error("Failed to read movement_obj transform")
           return False
  
   def start_continuous_monitoring(self, interval=0.1):
       """Continuously monitor movement_obj and send transforms to Kafka"""
       logger.info("Starting continuous monitoring of movement_obj with Y-Z coordinate transformation")
       logger.info("Sending to topic '%s' every %s seconds" % (self.topic, interval))
       logger.info("Press Ctrl+C to stop...")
      
       message_count = 0
      
       try:
           while True:
               if self.send_movement_cube_data():
                   message_count += 1
                   if message_count % 50 == 0:  # Log every 50 messages to avoid spam
                       logger.info("Sent %s messages to Kafka with coordinate transformation" % message_count)
              
               time.sleep(interval)
              
       except KeyboardInterrupt:
           logger.info("\nStopping monitoring... Sent %s total messages" % message_count)
           self.close()
       except Exception as e:
           logger.error("Error in continuous monitoring: %s" % e)
           self.close()
  
   def close(self):
       """Close Kafka producer connection"""
       if self.kafka_producer:
           logger.info("Closing Kafka producer...")
           self.kafka_producer.flush()  # Ensure all messages are sent
           self.kafka_producer.close()
           self.kafka_connected = False


# Example usage and testing
if __name__ == "__main__":
   producer = BLOmniverseKafkaProducer()
  
   # Test connections
   print("=== Omniverse Kafka Producer with Y-Z Coordinate Transformation ===")
   print("Testing connections...")
  
   # Connect to Kafka first
   if producer.connect_to_kafka():
       print("Kafka connection successful")
      
       # Test Kafka with a message
       if producer.test_kafka_connection():
           print("Kafka test message with coordinate transformation sent successfully")
       else:
           print("Kafka test message failed")
   else:
       print("Kafka connection failed")
       print("Make sure Kafka is running on 192.168.1.74:9092")
       exit(1)
  
   # Connect to Omniverse
   if producer.connect_to_omniverse():
       print("Omniverse connection successful")
   else:
       print("Omniverse connection failed")
       print("Make sure you're running this in Omniverse with movement_obj object")
       exit(1)
  
   # Test reading movement_cube
   print("\nTesting movement_obj reading with coordinate transformation...")
   hsml_data = producer.read_movement_cube_transform()
   if hsml_data:
       print("Successfully read movement_obj transform with Y-Z transformation:")
       print(json.dumps(hsml_data, indent=2))
      
       # Test sending to Kafka
       if producer.send_movement_cube_data():
           print("Successfully sent movement_obj data to Kafka with coordinate transformation!")
           print("Single test completed. Closing connections...")
       else:
           print("Failed to send movement_obj data to Kafka")
   else:
       print("Failed to read movement_obj transform")
       print("Make sure the movement_obj object exists at /World/CADREDemo/CADRE_Demo/Chassis")
  
   # Always close connections at the end
   producer.close()