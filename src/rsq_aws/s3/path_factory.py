from typing import Dict
from rsq_utils.paths import clean_path, find_template_params

class PathFactory():
    def __init__(self):
        self.valid_files = ["parquet", "json"]

    def generate(self, template_path: str, parameters: Dict[str, str]) -> str:
        self._validate_parameters(template_path, parameters)
        self._validate_file_type(template_path)
        
        path = self._format_path_with_parameters(template_path, parameters)
        path = clean_path(path)

        return path

    def _validate_parameters(self, template_path: str, parameters: Dict[str, str] = {}) -> None:
        """
        If template_path has template parameters, check if the parameters are in line with the parameters provided
        """
        template_params = find_template_params(template_path)
        
        if set(parameters.keys()) != set(template_params):
            raise Exception(f"Template parameters should match parameter keys provided")
        
        if template_params:
            for param in parameters.values():
                self.__param_is_string(param)
                self.__param_is_not_empty(param)
                self.__param_does_not_contain_forward_slash(param)
                self.__param_is_lowercase(param)
                self.__param_does_not_contain_spaces(param)                

    def _validate_file_type(self, template_path: str) -> None:
        """
        If the file type is provided, check if it is valid
        """
        if "." in template_path:
            file = template_path.split(".")[-1]
            if file not in self.valid_files:
                raise Exception(f"Invalid file type '{file}'. Accepted file types are {self.valid_files}")
        
    def _format_path_with_parameters(self, template_path: str, parameters: Dict[str, str]) -> str:
        return template_path.format(**parameters)

    def __param_is_string(self, param: str) ->None:
        if not isinstance(param, str):
            raise Exception(f"{param} must be a string")
        
    def __param_is_not_empty(self, param: str) -> None:
        if param == "":
            raise Exception(f"{param} cannot be empty")
        
    def __param_does_not_contain_forward_slash(self, param: str) -> None:
        if "/" in param:
            raise Exception(f"{param} cannot contain '/'")
        
    def __param_is_lowercase(self, param: str) -> None:
        if param.lower() != param:
            raise Exception(f"{param} must be in lowercase")
    
    def __param_does_not_contain_spaces(self, param: str) -> None:
        if param.replace(" ", "_") != param:
            raise Exception(f"{param} must not contain spaces")

