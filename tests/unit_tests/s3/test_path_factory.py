from typing import Dict
import pytest
from src.rsq_aws.s3.path_factory import PathFactory

@pytest.fixture
def path_factory() -> PathFactory:
    """Fixture for PathFactory instance."""
    return PathFactory()

def test_init(path_factory: PathFactory) -> None:
    """Test initialization of PathFactory."""
    assert path_factory.valid_files == ["parquet", "json"]

def test_generate_valid_path(path_factory: PathFactory) -> None:
    """Test generating valid paths with parameters."""
    template = "{year}/{month}/data.json"
    params: Dict[str, str] = {"year": "2024", "month": "02"}
    result = path_factory.generate(template, params)
    assert result == "2024/02/data.json"

def test_generate_folder_path(path_factory: PathFactory) -> None:
    """Test generating folder paths."""
    template = "{region}/data/{date}"
    params: Dict[str, str] = {"region": "us", "date": "20240217"}
    result = path_factory.generate(template, params)
    assert result == "us/data/20240217/"

def test_validate_file_type_valid(path_factory: PathFactory) -> None:
    """Test validating supported file types."""
    # Test JSON
    path_factory._validate_file_type("file.json")
    # Test Parquet
    path_factory._validate_file_type("file.parquet")
    # Test path without extension
    path_factory._validate_file_type("folder/subfolder")

def test_validate_file_type_invalid(path_factory: PathFactory) -> None:
    """Test validating unsupported file types."""
    with pytest.raises(Exception) as exc_info:
        path_factory._validate_file_type("file.txt")
    assert "Invalid file type" in str(exc_info.value)
    assert "Accepted file types are" in str(exc_info.value)

def test_validate_parameters_missing(path_factory: PathFactory) -> None:
    """Test validation with missing parameters."""
    template = "{year}/{month}/data.json"
    params: Dict[str, str] = {"year": "2024"}  # missing month parameter
    with pytest.raises(Exception) as exc_info:
        path_factory.generate(template, params)
    assert "Template parameters should match" in str(exc_info.value)

def test_validate_parameters_extra(path_factory: PathFactory) -> None:
    """Test validation with extra parameters."""
    template = "{year}/data.json"
    params: Dict[str, str] = {"year": "2024", "extra": "value"}
    with pytest.raises(Exception) as exc_info:
        path_factory.generate(template, params)
    assert "Template parameters should match" in str(exc_info.value)

def test_parameter_validation_not_string(path_factory: PathFactory) -> None:
    """Test parameter validation for non-string values."""
    template = "{value}/data.json"
    params: Dict[str, object] = {"value": 123}  # number instead of string
    with pytest.raises(Exception) as exc_info:
        path_factory.generate(template, params)
    assert "must be a string" in str(exc_info.value)

def test_parameter_validation_empty(path_factory: PathFactory) -> None:
    """Test parameter validation for empty strings."""
    template = "{folder}/data.json"
    params: Dict[str, str] = {"folder": ""}
    with pytest.raises(Exception) as exc_info:
        path_factory.generate(template, params)
    assert "cannot be empty" in str(exc_info.value)

def test_parameter_validation_forward_slash(path_factory: PathFactory) -> None:
    """Test parameter validation for forward slashes."""
    template = "{path}/data.json"
    params: Dict[str, str] = {"path": "invalid/path"}
    with pytest.raises(Exception) as exc_info:
        path_factory.generate(template, params)
    assert "cannot contain '/'" in str(exc_info.value)

def test_parameter_validation_lowercase(path_factory: PathFactory) -> None:
    """Test parameter validation for lowercase requirement."""
    template = "{name}/data.json"
    params: Dict[str, str] = {"name": "UPPERCASE"}
    with pytest.raises(Exception) as exc_info:
        path_factory.generate(template, params)
    assert "must be in lowercase" in str(exc_info.value)

def test_parameter_validation_spaces(path_factory: PathFactory) -> None:
    """Test parameter validation for spaces."""
    template = "{name}/data.json"
    params: Dict[str, str] = {"name": "invalid name"}
    with pytest.raises(Exception) as exc_info:
        path_factory.generate(template, params)
    assert "must not contain spaces" in str(exc_info.value)

def test_format_path_with_parameters(path_factory: PathFactory) -> None:
    """Test path formatting with valid parameters."""
    template = "{region}/{year}/{month}/data_{type}.{format}"
    params: Dict[str, str] = {
        "region": "us",
        "year": "2024",
        "month": "02",
        "type": "sales",
        "format": "json"
    }
    result = path_factory._format_path_with_parameters(template, params)
    assert result == "us/2024/02/data_sales.json"
