import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

from clinical_inventory.raw.decrypt_file import AESDecryptor


class TestAESDecryptor:
    def test_instantiation(self):
        # 16-byte key for AES-128
        key = "a" * 16
        decryptor = AESDecryptor(key)
        assert decryptor is not None
