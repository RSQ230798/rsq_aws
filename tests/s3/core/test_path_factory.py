import unittest
from src.rsq_aws.s3.core.path_factory import PathFactory

class TestPathFactory(unittest.TestCase):
    """Test suite for PathFactory class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.path_factory = PathFactory()

    def test_init(self):
        """Test initialization of PathFactory."""
        self.assertEqual(self.path_factory.valid_files, ["parquet", "json"])

    def test_generate_valid_path(self):
        """Test generating valid paths with parameters."""
        template = "{year}/{month}/data.json"
        params = {"year": "2024", "month": "02"}
        result = self.path_factory.generate(template, params)
        self.assertEqual(result, "2024/02/data.json")

    def test_generate_folder_path(self):
        """Test generating folder paths."""
        template = "{region}/data/{date}"
        params = {"region": "us", "date": "20240217"}
        result = self.path_factory.generate(template, params)
        self.assertEqual(result, "us/data/20240217/")

    def test_validate_file_type_valid(self):
        """Test validating supported file types."""
        # Test JSON
        self.path_factory._validate_file_type("file.json")
        # Test Parquet
        self.path_factory._validate_file_type("file.parquet")
        # Test path without extension
        self.path_factory._validate_file_type("folder/subfolder")

    def test_validate_file_type_invalid(self):
        """Test validating unsupported file types."""
        with self.assertRaises(Exception) as context:
            self.path_factory._validate_file_type("file.txt")
        self.assertIn("Invalid file type", str(context.exception))
        self.assertIn("Accepted file types are", str(context.exception))

    def test_validate_parameters_missing(self):
        """Test validation with missing parameters."""
        template = "{year}/{month}/data.json"
        params = {"year": "2024"}  # missing month parameter
        with self.assertRaises(Exception) as context:
            self.path_factory.generate(template, params)
        self.assertIn("Template parameters should match", str(context.exception))

    def test_validate_parameters_extra(self):
        """Test validation with extra parameters."""
        template = "{year}/data.json"
        params = {"year": "2024", "extra": "value"}
        with self.assertRaises(Exception) as context:
            self.path_factory.generate(template, params)
        self.assertIn("Template parameters should match", str(context.exception))

    def test_parameter_validation_not_string(self):
        """Test parameter validation for non-string values."""
        template = "{value}/data.json"
        params = {"value": 123}  # number instead of string
        with self.assertRaises(Exception) as context:
            self.path_factory.generate(template, params)
        self.assertIn("must be a string", str(context.exception))

    def test_parameter_validation_empty(self):
        """Test parameter validation for empty strings."""
        template = "{folder}/data.json"
        params = {"folder": ""}
        with self.assertRaises(Exception) as context:
            self.path_factory.generate(template, params)
        self.assertIn("cannot be empty", str(context.exception))

    def test_parameter_validation_forward_slash(self):
        """Test parameter validation for forward slashes."""
        template = "{path}/data.json"
        params = {"path": "invalid/path"}
        with self.assertRaises(Exception) as context:
            self.path_factory.generate(template, params)
        self.assertIn("cannot contain '/'", str(context.exception))

    def test_parameter_validation_lowercase(self):
        """Test parameter validation for lowercase requirement."""
        template = "{name}/data.json"
        params = {"name": "UPPERCASE"}
        with self.assertRaises(Exception) as context:
            self.path_factory.generate(template, params)
        self.assertIn("must be in lowercase", str(context.exception))

    def test_parameter_validation_spaces(self):
        """Test parameter validation for spaces."""
        template = "{name}/data.json"
        params = {"name": "invalid name"}
        with self.assertRaises(Exception) as context:
            self.path_factory.generate(template, params)
        self.assertIn("must not contain spaces", str(context.exception))

    def test_format_path_with_parameters(self):
        """Test path formatting with valid parameters."""
        template = "{region}/{year}/{month}/data_{type}.{format}"
        params = {
            "region": "us",
            "year": "2024",
            "month": "02",
            "type": "sales",
            "format": "json"
        }
        result = self.path_factory._format_path_with_parameters(template, params)
        self.assertEqual(result, "us/2024/02/data_sales.json")


if __name__ == '__main__':
    unittest.main()
