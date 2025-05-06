import pytest
import json
import base64
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from app.authentication.AccessTokenValidator import AccessTokenValidator


@pytest.fixture
def mock_key_manager():
    with patch('app.authentication.AccessTokenValidator.KeyManager') as mock:
        yield mock


@pytest.fixture
def rsa_key_pair():
    # Generate a private key for testing
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    public_key = private_key.public_key()
    return private_key, public_key


@pytest.fixture
def create_token(rsa_key_pair):
    private_key, public_key = rsa_key_pair
    
    def _create_token(payload, key_id="test_key_id", expired=False):
        if expired:
            exp_time = int((datetime.now() - timedelta(hours=1)).timestamp())
        else:
            exp_time = int((datetime.now() + timedelta(hours=1)).timestamp())
        
        # Add expiration to payload
        payload["exp"] = exp_time
        
        # Create header
        header = {
            "alg": "RS256",
            "typ": "JWT",
            "kid": key_id
        }
        
        # Encode header and payload
        header_json = json.dumps(header).encode()
        header_b64 = base64.urlsafe_b64encode(header_json).decode().rstrip("=")
        
        payload_json = json.dumps(payload).encode()
        payload_b64 = base64.urlsafe_b64encode(payload_json).decode().rstrip("=")
        
        # Create signature
        message = f"{header_b64}.{payload_b64}".encode()
        signature = private_key.sign(
            message,
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        signature_b64 = base64.urlsafe_b64encode(signature).decode().rstrip("=")
        
        # Return the complete token
        return f"{header_b64}.{payload_b64}.{signature_b64}"
    
    return _create_token


def test_validate_token_valid(mock_key_manager, rsa_key_pair, create_token):
    # Setup
    _, public_key = rsa_key_pair
    mock_key_manager.get_public_key.return_value = public_key
    
    payload = {
        "sub": "user:123456",
        "iss": "https://example.com/realms/test-realm",
        "name": "Test User"
    }
    
    token = create_token(payload)
    
    # Execute
    result = AccessTokenValidator.validate_token(token, True)
    
    # Verify
    assert result == payload
    assert result.get("sub") == "user:123456"
    mock_key_manager.get_public_key.assert_called_once()


def test_validate_token_expired(mock_key_manager, rsa_key_pair, create_token):
    # Setup
    _, public_key = rsa_key_pair
    mock_key_manager.get_public_key.return_value = public_key
    
    payload = {
        "sub": "user:123456",
        "iss": "https://example.com/realms/test-realm"
    }
    
    token = create_token(payload, expired=True)
    
    # Execute
    result = AccessTokenValidator.validate_token(token, True)
    
    # Verify
    assert result == {}


def test_validate_token_ignore_expiration(mock_key_manager, rsa_key_pair, create_token):
    # Setup
    _, public_key = rsa_key_pair
    mock_key_manager.get_public_key.return_value = public_key
    
    payload = {
        "sub": "user:123456",
        "iss": "https://example.com/realms/test-realm"
    }
    
    token = create_token(payload, expired=True)
    
    # Execute - with check_active=False
    result = AccessTokenValidator.validate_token(token, False)
    
    # Verify - should return payload even though token is expired
    assert result.get("sub") == "user:123456"


def test_validate_token_invalid_signature(mock_key_manager, create_token):
    # Setup - use a different key for verification than was used for signing
    different_public_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    ).public_key()
    
    mock_key_manager.get_public_key.return_value = different_public_key
    
    payload = {"sub": "user:123456"}
    token = create_token(payload)
    
    # Execute
    result = AccessTokenValidator.validate_token(token, True)
    
    # Verify
    assert result == {}


def test_validate_token_invalid_format():
    # Setup - malformed token
    token = "header.payload"  # Missing signature part
    
    # Execute
    result = AccessTokenValidator.validate_token(token, True)
    
    # Verify
    assert result == {}


def test_verify_user_token_valid(mock_key_manager, rsa_key_pair, create_token):
    # Setup
    _, public_key = rsa_key_pair
    mock_key_manager.get_public_key.return_value = public_key
    
    with patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_URL', 'https://example.com/'), \
         patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_REALM', 'test-realm'):
        
        payload = {
            "sub": "user:123456",
            "iss": "https://example.com/realms/test-realm"
        }
        
        token = create_token(payload)
        
        # Execute
        result = AccessTokenValidator.verify_user_token(token, True)
        
        # Verify
        assert result == "123456"


def test_verify_user_token_invalid_issuer(mock_key_manager, rsa_key_pair, create_token):
    # Setup
    _, public_key = rsa_key_pair
    mock_key_manager.get_public_key.return_value = public_key
    
    with patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_URL', 'https://example.com/'), \
         patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_REALM', 'test-realm'):
        
        payload = {
            "sub": "user:123456",
            "iss": "https://wrong-issuer.com/realms/test-realm"
        }
        
        token = create_token(payload)
        
        # Execute
        result = AccessTokenValidator.verify_user_token(token, True)
        
        # Verify
        assert result == "UNAUTHORIZED"


def test_verify_user_token_no_sub(mock_key_manager, rsa_key_pair, create_token):
    # Setup
    _, public_key = rsa_key_pair
    mock_key_manager.get_public_key.return_value = public_key
    
    with patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_URL', 'https://example.com/'), \
         patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_REALM', 'test-realm'):
        
        payload = {
            "iss": "https://example.com/realms/test-realm"
            # No "sub" field
        }
        
        token = create_token(payload)
        
        # Execute
        result = AccessTokenValidator.verify_user_token(token, True)
        
        # Verify
        assert result == "UNAUTHORIZED"


def test_verify_user_token_exception():
    # Setup - pass None as token to trigger exception
    token = None
    
    # Execute
    result = AccessTokenValidator.verify_user_token(token, True)
    
    # Verify
    assert result == "UNAUTHORIZED"


def test_verify_user_token_get_org_valid(mock_key_manager, rsa_key_pair, create_token):
    # Setup
    _, public_key = rsa_key_pair
    mock_key_manager.get_public_key.return_value = public_key
    
    with patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_URL', 'https://example.com/'), \
         patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_REALM', 'test-realm'):
        
        payload = {
            "sub": "user:123456",
            "iss": "https://example.com/realms/test-realm",
            "org": "org123"
        }
        
        token = create_token(payload)
        
        # Execute
        result = AccessTokenValidator.verify_user_token_get_org(token, True)
        
        # Verify
        assert result == "org123"


def test_verify_user_token_get_org_no_org(mock_key_manager, rsa_key_pair, create_token):
    # Setup
    _, public_key = rsa_key_pair
    mock_key_manager.get_public_key.return_value = public_key
    
    with patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_URL', 'https://example.com/'), \
         patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_REALM', 'test-realm'):
        
        payload = {
            "sub": "user:123456",
            "iss": "https://example.com/realms/test-realm"
            # No "org" field
        }
        
        token = create_token(payload)
        
        # Execute
        result = AccessTokenValidator.verify_user_token_get_org(token, True)
        
        # Verify
        assert result == ""


def test_verify_user_token_get_org_exception():
    # Setup - pass None as token to trigger exception
    token = None
    
    # Execute
    result = AccessTokenValidator.verify_user_token_get_org(token, True)
    
    # Verify
    assert result == ""


def test_check_iss_valid():
    # Setup
    with patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_URL', 'https://example.com/'), \
         patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_REALM', 'test-realm'):
        
        iss = "https://example.com/realms/test-realm"
        
        # Execute
        result = AccessTokenValidator.check_iss(iss)
        
        # Verify
        assert result is True


def test_check_iss_invalid():
    # Setup
    with patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_URL', 'https://example.com/'), \
         patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_REALM', 'test-realm'):
        
        iss = "https://wrong-issuer.com/realms/test-realm"
        
        # Execute
        result = AccessTokenValidator.check_iss(iss)
        
        # Verify
        assert result is False


def test_check_iss_case_insensitive():
    # Setup
    with patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_URL', 'https://example.com/'), \
         patch('app.authentication.AccessTokenValidator.SUNBIRD_SSO_REALM', 'test-realm'):
        
        iss = "HTTPS://EXAMPLE.COM/REALMS/TEST-REALM"
        
        # Execute
        result = AccessTokenValidator.check_iss(iss)
        
        # Verify
        assert result is True


def test_is_expired_true():
    # Setup - time in the past
    expiration = (datetime.now() - timedelta(hours=1)).timestamp()
    
    # Execute
    result = AccessTokenValidator.is_expired(expiration)
    
    # Verify
    assert result is True


def test_is_expired_false():
    # Setup - time in the future
    expiration = (datetime.now() + timedelta(hours=1)).timestamp()
    
    # Execute
    result = AccessTokenValidator.is_expired(expiration)
    
    # Verify
    assert result is False