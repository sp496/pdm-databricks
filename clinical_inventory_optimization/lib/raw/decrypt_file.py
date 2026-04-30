import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad


class AESDecryptor:
    """
    Reusable AES decryption class that auto-detects encryption mode and format.
    Supports CBC and ECB modes with Base64 or binary encoded files.
    """

    def __init__(self, key_b64, debug=False):
        """
        Initialize decryptor with Base64-encoded key.

        Args:
            key_b64 (str): Base64-encoded AES key
            debug (bool): Enable debug output
        """
        self.key = base64.b64decode(key_b64)
        self.debug = debug

        # Validate key length
        if len(self.key) not in [16, 24, 32]:
            raise ValueError(f"Invalid key length: {len(self.key)} bytes (expected 16, 24, or 32)")

        if self.debug:
            print(f"✅ Initialized with AES-{len(self.key) * 8} key")

    def _detect_format(self, raw_data):
        """
        Detect encryption format and mode from raw file data.

        Returns:
            tuple: (mode, iv, ciphertext)
        """
        try:
            data_text = raw_data.decode("utf-8")

            if ":" in data_text:
                # Format: Base64_IV:Base64_Ciphertext (CBC mode)
                iv_b64, ciphertext_b64 = [x.strip() for x in data_text.split(":", 1)]
                iv = base64.b64decode(iv_b64)
                ciphertext = base64.b64decode(ciphertext_b64)
                mode = "CBC"
            else:
                # Base64 ciphertext only (ECB mode)
                ciphertext = base64.b64decode(data_text)
                iv = None
                mode = "ECB"

        except (UnicodeDecodeError, Exception):
            # Binary file
            if len(raw_data) % 16 == 0:
                # ECB mode (no IV)
                ciphertext = raw_data
                iv = None
                mode = "ECB"
            else:
                # Assume CBC with IV as first 16 bytes
                iv = raw_data[:16]
                ciphertext = raw_data[16:]
                mode = "CBC"

        if self.debug:
            print(f"ℹ️ Detected mode: {mode}")
            if mode == "ECB":
                print("⚠️ WARNING: ECB mode is not cryptographically secure")

        return mode, iv, ciphertext

    def _safe_unpad(self, data):
        """
        Safely remove padding from decrypted data.
        Falls back to zero-padding removal if PKCS7 fails.
        """
        try:
            return unpad(data, AES.block_size)
        except ValueError:
            if self.debug:
                print("⚠️ PKCS7 unpad failed, using zero-padding fallback")
            return data.rstrip(b'\x00')

    def _detect_file_type(self, data):
        """
        Detect the type of decrypted file.

        Returns:
            str: File type description
        """
        if data[:4] == b'PK\x03\x04':
            return "xlsx"
        elif data[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
            return "xls"
        else:
            try:
                sample = data[:200].decode('utf-8')
                if ',' in sample or '\t' in sample:
                    return "csv"
            except:
                pass
            return "unknown"

    def _derive_output_path(self, input_path, file_type):
        """
        Derive output path from input path by stripping .enc and appending
        the correct extension based on detected file type.
        """
        import os
        base = input_path
        if base.lower().endswith(".enc"):
            base = base[:-4]
        root, _ = os.path.splitext(base)
        ext_map = {"xlsx": ".xlsx", "xls": ".xls", "csv": ".csv"}
        ext = ext_map.get(file_type, os.path.splitext(base)[1] or "")
        return root + ext

    def decrypt_file(self, input_path, output_path=None):
        """
        Decrypt a file from input_path and save to output_path.

        If output_path is not provided, it is derived from input_path by
        stripping the .enc extension and using the detected file type
        (xlsx, xls, or csv) to set the correct extension.

        Args:
            input_path (str): Path to encrypted file
            output_path (str | None): Path to save decrypted file; auto-derived if omitted

        Returns:
            dict: Decryption metadata
        """
        try:
            # Read encrypted file
            with open(input_path, "rb") as f:
                raw_data = f.read().strip()

            if self.debug:
                print(f"📂 Read {len(raw_data)} bytes from {input_path}")

            # Detect format
            mode, iv, ciphertext = self._detect_format(raw_data)

            # Create cipher
            if mode == "CBC":
                cipher = AES.new(self.key, AES.MODE_CBC, iv)
            else:
                cipher = AES.new(self.key, AES.MODE_ECB)

            # Decrypt
            decrypted_data = cipher.decrypt(ciphertext)
            decrypted_data = self._safe_unpad(decrypted_data)

            if self.debug:
                print(f"🔹 First 32 bytes of decrypted data (hex):")
                print(decrypted_data[:32].hex())

            # Attempt Base64 decode of decrypted data
            try:
                final_data = base64.b64decode(decrypted_data)
                base64_decoded = True
                if self.debug:
                    print("ℹ️ Base64 decoding successful")
            except Exception:
                final_data = decrypted_data
                base64_decoded = False
                if self.debug:
                    print("⚠️ Base64 decode failed, using raw decrypted data")

            # Detect file type
            file_type = self._detect_file_type(final_data)
            if self.debug:
                print(f"✅ Detected file type: {file_type}")

            # Resolve output path: auto-derive when None or no extension given
            import os as _os
            if output_path is None:
                output_path = self._derive_output_path(input_path, file_type)
            elif not _os.path.splitext(output_path)[1]:
                output_path = self._derive_output_path(output_path, file_type)

            # Save decrypted file
            with open(output_path, "wb") as f:
                f.write(final_data)

            print(f"✅ Decryption complete — saved as: {output_path}")

            return {
                "success": True,
                "mode": mode,
                "base64_decoded": base64_decoded,
                "file_type": file_type,
                "output_size": len(final_data)
            }

        except FileNotFoundError:
            print(f"❌ Error: Input file not found: {input_path}")
            return {"success": False, "error": "File not found"}
        except Exception as e:
            print(f"❌ Decryption error: {str(e)}")
            return {"success": False, "error": str(e)}

    def decrypt_bytes(self, encrypted_bytes):
        """
        Decrypt raw bytes and return decrypted bytes.
        Useful for in-memory operations in Databricks.

        Args:
            encrypted_bytes (bytes): Encrypted data

        Returns:
            bytes: Decrypted data
        """
        mode, iv, ciphertext = self._detect_format(encrypted_bytes)

        if mode == "CBC":
            cipher = AES.new(self.key, AES.MODE_CBC, iv)
        else:
            cipher = AES.new(self.key, AES.MODE_ECB)

        decrypted_data = cipher.decrypt(ciphertext)
        decrypted_data = self._safe_unpad(decrypted_data)

        # Attempt Base64 decode
        try:
            return base64.b64decode(decrypted_data)
        except Exception:
            return decrypted_data


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

# Example 1: Single file decryption
if __name__ == "__main__":
    key = ""

    decryptor = AESDecryptor(key, debug=False)

    # output_path is optional — extension is auto-detected from decrypted content
    result = decryptor.decrypt_file(
        "GS-US-592-6173 CustomReport4_SubjectSummary_04-Sep-2025_100250_extcsv.enc"
    )

    print(f"\nDecryption result: {result}")
