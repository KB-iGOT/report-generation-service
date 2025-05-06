import pytest
import os
import tempfile
import base64
from unittest.mock import patch, mock_open, MagicMock
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from app.authentication.KeyManager import KeyManager


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test key files."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.fixture
def rsa_key_pair():
    """Generate an RSA key pair for testing."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    public_key = private_key.public_key()
    
    # Get the PEM format
    public_key_der = public_key.public_bytes(
        encoding=Encoding.DER,
        format=PublicFormat.SubjectPublicKeyInfo
    )
    public_key_b64 = base64.b64encode(public_key_der).decode('utf-8')
    public_key_pem = f"-----BEGIN PUBLIC KEY-----\n{public_key_b64}\n-----END PUBLIC KEY-----"
    
    return private_key, public_key, public_key_pem


def test_init_with_valid_keys(temp_dir, rsa_key_pair):
    """Test initializing KeyManager with valid key files."""
    # Setup
    _, _, public_key_pem = rsa_key_pair
    
    # Create test key files
    key_file1 = os.path.join(temp_dir, "key1")
    key_file2 = os.path.join(temp_dir, "key2")
    
    with open(key_file1, "w") as f:
        f.write(public_key_pem)
    
    with open(key_file2, "w") as f:
        f.write(public_key_pem)
    
    # Reset key_map before test
    KeyManager.key_map = {}
    
    # Execute
    KeyManager.init(temp_dir)
    
    # Verify
    assert len(KeyManager.key_map) == 2
    assert "key1" in KeyManager.key_map
    assert "key2" in KeyManager.key_map


def test_init_with_invalid_key_file(temp_dir):
    """Test initializing KeyManager with an invalid key file."""
    # Setup
    invalid_key_file = os.path.join(temp_dir, "invalid_key")
    
    with open(invalid_key_file, "w") as f:
        f.write("This is not a valid public key")
    
    # Reset key_map before test
    KeyManager.key_map = {}
    
    # Execute
    with patch('logging.Logger.error') as mock_logger:
        KeyManager.init(temp_dir)
    
    # Verify
    assert len(KeyManager.key_map) == 0
    mock_logger.assert_called()


def test_init_with_directory_error():
    """Test initializing KeyManager with a non-existent directory."""
    # Setup - non-existent directory
    non_existent_dir = "/non/existent/directory"
    
    # Reset key_map before test
    KeyManager.key_map = {}
    
    # Execute
    with patch('os.walk') as mock_walk, patch('logging.Logger.error') as mock_logger:
        mock_walk.side_effect = Exception("Directory error")
        KeyManager.init(non_existent_dir)
    
    # Verify
    assert len(KeyManager.key_map) == 0
    mock_logger.assert_called_once()


def test_get_public_key_existing():
    """Test getting an existing public key."""
    # Setup
    mock_key = MagicMock()
    KeyManager.key_map = {"test_key": mock_key}
    
    # Execute
    result = KeyManager.get_public_key("test_key")
    
    # Verify
    assert result == mock_key


def test_get_public_key_non_existing():
    """Test getting a non-existing public key."""
    # Setup
    KeyManager.key_map = {"test_key": MagicMock()}
    
    # Execute
    result = KeyManager.get_public_key("non_existing_key")
    
    # Verify
    assert result is None


def test_load_public_key_valid(rsa_key_pair):
    """Test loading a valid public key."""
    # Setup
    _, expected_public_key, public_key_pem = rsa_key_pair
    
    # Execute
    result = KeyManager.load_public_key(public_key_pem)
    
    # Verify - compare the DER representation of both keys
    result_der = result.public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    expected_der = expected_public_key.public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    assert result_der == expected_der


def test_load_public_key_invalid():
    """Test loading an invalid public key."""
    # Setup
    invalid_key = "This is not a valid public key"
    
    # Execute and Verify
    with pytest.raises(Exception):
        KeyManager.load_public_key(invalid_key)


def test_load_public_key_with_whitespace(rsa_key_pair):
    """Test loading a public key with extra whitespace."""
    # Setup
    _, expected_public_key, public_key_pem = rsa_key_pair
    # Add extra whitespace
    public_key_pem_with_whitespace = f"  {public_key_pem}  \n  "
    
    # Execute
    result = KeyManager.load_public_key(public_key_pem_with_whitespace)
    
    # Verify
    result_der = result.public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    expected_der = expected_public_key.public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    assert result_der == expected_der