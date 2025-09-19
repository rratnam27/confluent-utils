import unittest
from src.confluent_utils.enums import DeliveryResultEnum


class TestDeliveryResultEnum(unittest.TestCase):
    def test_enum_members_exist(self):
        self.assertEqual(DeliveryResultEnum.KEY_STATUS, "status")
        self.assertEqual(DeliveryResultEnum.KEY_TOPIC, "topic")
        self.assertEqual(DeliveryResultEnum.KEY_PARTITION, "partition")
        self.assertEqual(DeliveryResultEnum.KEY_OFFSET, "offset")
        self.assertEqual(DeliveryResultEnum.VALUE_SUCCESS, "success")


if __name__ == "__main__":
    unittest.main()
