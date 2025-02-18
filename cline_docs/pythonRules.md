# Project-Specific Rules

## Language: Python

### Interface Pattern
```python
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

class PaymentProcessorInterface(ABC):
    """
    Handles payment processing operations.
    """
    @abstractmethod
    def process_payment(
        self, 
        amount: float, 
        user_id: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """Process a payment transaction."""
        pass

    @abstractmethod
    def get_transaction_status(
        self, 
        transaction_id: str
    ) -> Dict[str, Any]:
        """Retrieve status of a transaction."""
        pass
```

### Class Implementation Pattern
```python
from typing import Protocol
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

### Type Requirements
- All variables must have type labels
- All parameters must have type labels
- All function returns must have type labels
- Use dataclasses for data structures
- Use Protocols for duck typing

### Testing Pattern
```python
from unittest import TestCase
from unittest.mock import Mock

class TestPaymentProcessor(TestCase):
    def setUp(self) -> None:
        self.mock_gateway: Mock = Mock(spec=PaymentGatewayInterface)
        self.processor: PaymentProcessor = PaymentProcessor(self.mock_gateway)

    def test_successful_payment(self) -> None:
        # Arrange
        self.mock_gateway.charge.return_value = True
        
        # Act
        result: bool = self.processor.process_payment(
            amount=100.0,
            user_id="user123"
        )
        
        # Assert
        self.assertTrue(result)
        self.mock_gateway.charge.assert_called_once()
```

### Testing Requirements
- Unit test coverage: 100%
- Integration test coverage: 80%
- Max test execution time: 10 minutes
- Use Arrange-Act-Assert pattern
- Mock external dependencies on most tests
- External dependencies must be tested

### Project-Specific Patterns
- Use dependency injection
- Follow repository pattern for data access
- Use factory pattern for object creation
- Use dataclasses for data structures
- Use Protocols over ABC when possible

### Documentation Requirements
- OpenAPI spec for REST endpoints
- Sequence diagrams for complex flows
- Performance benchmarks
