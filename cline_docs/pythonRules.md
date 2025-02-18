# Python Specific Rules
## Global Requirements
**CRITICAL**
- Type hints are strictly required.
- Clear doc strings are required (include examples when possible).
- Limit comments.
- Use dependency injection.
- Use factory pattern for object creation.
- Use dataclasses for data structures.
- Use protocols over ABC when possible.

### Class Implementation Pattern
```python
from typing import Protocol, Dict
from dataclasses import dataclass

@dataclass
class PaymentDetails:
    amount: float
    user_id: str
    metadata: Optional[Dict[str, str]] = None

class PaymentGatewayInterface(Protocol):
    def charge(self, details: PaymentDetails) -> bool: ...
    def get_status(self, transaction_id: str) -> Dict[str, Any]: ...

class PaymentProcessor:
    def __init__(self, payment_gateway: PaymentGatewayInterface) -> None:
        self._gateway: PaymentGatewayInterface = payment_gateway
        self._retry_count: int = 3

    def process_payment(
        self, 
        amount: float,
        user_id: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        details: PaymentDetails = PaymentDetails(
            amount=amount,
            user_id=user_id,
            metadata=metadata
        )
        return self._gateway.charge(details)
```

### Type Hint Requirements
- All variables must have type labels
- All parameters must have type labels
- All function returns must have type labels

### Testing Requirements
- Use Pytest
- Use mypy to test type hints
- Unit test coverage: 100%
- Max test execution time: 10 minutes
- Mock all external dependencies on unit tests
- Test all external dependencies with integration tests

### Test Implementation Pattern
```python
import pytest
from unittest.mock import Mock
from typing import Generator

# Fixtures follow dependency injection pattern
@pytest.fixture
def mock_gateway() -> Generator[PaymentGatewayInterface, None, None]:
    gateway = Mock()
    gateway.charge.return_value = True
    gateway.get_status.return_value = {"status": "success"}
    yield gateway

# Unit tests mock external dependencies
def test_payment_processor_success(mock_gateway: PaymentGatewayInterface) -> None:
    # Arrange
    processor = PaymentProcessor(payment_gateway=mock_gateway)
    test_amount = 100.0
    test_user = "user123"
    
    # Act
    result = processor.process_payment(
        amount=test_amount,
        user_id=test_user
    )
    
    # Assert
    assert result is True
    mock_gateway.charge.assert_called_once()
    
# Integration tests verify external dependencies
@pytest.mark.integration
def test_payment_processor_integration() -> None:
    # Arrange
    real_gateway = RealPaymentGateway()
    processor = PaymentProcessor(payment_gateway=real_gateway)
    
    # Act
    result = processor.process_payment(
        amount=1.0,  # Use minimal amount for testing
        user_id="test_user"
    )
    
    # Assert
    assert result is True

# Type hint testing with mypy
# Run: mypy test_payment_processor.py
```

### Endpoint Structure
- OpenAPI spec for REST endpoints
