from modules.utils.logger import get_logger
from modules.utils.dbutils import get_dbutils
from typing import Any, Dict, List, Optional, Union
import json

logger = get_logger(__name__)
dbutils = get_dbutils()


def _convert_widget_value(value: str, target_type: Optional[type] = None) -> Any:
    """
    Convert widget string value to appropriate Python type.
    
    Args:
        value (str): Widget value as string
        target_type (type, optional): Target type to convert to
    
    Returns:
        Converted value
    
    Examples:
        >>> _convert_widget_value("true", bool)  # True
        >>> _convert_widget_value("false", bool)  # False
        >>> _convert_widget_value("123", int)  # 123
        >>> _convert_widget_value('["a","b"]', list)  # ['a', 'b']
        >>> _convert_widget_value("some_string", str)  # "some_string"
        >>> _convert_widghet_value('{"key": "value"}', dict)  # {'key': 'value'}
    """
    if target_type is None or target_type == str:
        return value
    
    # Boolean conversion
    if target_type == bool:
        if value.lower() in ('true', '1', 'yes', 'on'):
            return True
        elif value.lower() in ('false', '0', 'no', 'off', ''):
            return False
        else:
            raise ValueError(f"Cannot convert '{value}' to boolean")
    
    # Integer conversion
    if target_type == int:
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"Cannot convert '{value}' to integer")
    
    # Float conversion
    if target_type == float:
        try:
            return float(value)
        except ValueError:
            raise ValueError(f"Cannot convert '{value}' to float")
    
    # List/Dict conversion (JSON parsing)
    if target_type in (list, dict):
        try:
            parsed = json.loads(value)
            if not isinstance(parsed, target_type):
                raise ValueError(f"Parsed value is {type(parsed).__name__}, expected {target_type.__name__}")
            return parsed
        except json.JSONDecodeError as e:
            raise ValueError(f"Cannot parse '{value}' as JSON: {e}")
    
    # Default: try direct conversion
    try:
        return target_type(value)
    except Exception as e:
        raise ValueError(f"Cannot convert '{value}' to {target_type.__name__}: {e}")


def validate_required_widgets(
    required_widgets: List[str],
    widget_types: Optional[Dict[str, type]] = None
) -> Dict[str, Any]:
    """
    Validate that required Databricks widgets exist and are not empty.
    
    Args:
        required_widgets (list[str]): List of required widget names
        widget_types (dict, optional): Dictionary mapping widget names to target types
            Example: {'is_pii': bool, 'partition_by': list, 'max_records': int}
    
    Returns:
        dict: A dictionary containing validated and converted widgets
    
    Raises:
        ValueError: If any required widget is missing, empty, null, or cannot be converted
    
    Example:
        >>> # Simple validation (all strings)
        >>> widgets = validate_required_widgets(['workspace_path', 'bundle_target'])
        >>> 
        >>> # With type conversion
        >>> widgets = validate_required_widgets(
        ...     required_widgets=['workspace_path', 'is_pii', 'partition_by'],
        ...     widget_types={
        ...         'is_pii': bool,
        ...         'partition_by': list
        ...     }
        ... )
        >>> print(widgets['is_pii'])  # True (boolean, not string "true")
        >>> print(widgets['partition_by'])  # ['ingestion_date'] (list, not string)
    """

    logger.info(f"Validating required widgets: {required_widgets}")

    all_widgets = dbutils.widgets.getAll()
    validated_widgets = {}
    errors = []

    logger.info(f"All widgets found: {list(all_widgets.keys())}")
    
    for widget_name in required_widgets:
        # Check if widget exists
        if widget_name not in all_widgets:
            logger.error(f"Missing required widget: '{widget_name}'")
            errors.append(f"Missing required widget: '{widget_name}'")
            continue
        
        # Get widget value (always a string from DAB)
        widget_value = all_widgets[widget_name]
        
        # Check if value is None
        if widget_value is None:
            logger.error(f"Widget '{widget_name}' is null")
            errors.append(f"Widget '{widget_name}' is null")
            continue
        
        # Check if value is empty string (including whitespace-only strings)
        if isinstance(widget_value, str) and not widget_value.strip():
            logger.error(f"Widget '{widget_name}' is empty or contains only whitespace")
            errors.append(f"Widget '{widget_name}' is empty or contains only whitespace")
            continue
        
        # Convert to target type if specified
        if widget_types and widget_name in widget_types:
            target_type = widget_types[widget_name]
            try:
                converted_value = _convert_widget_value(widget_value, target_type)
                logger.info(
                    f"Widget '{widget_name}' converted from '{widget_value}' ({type(widget_value).__name__}) "
                    f"to {converted_value} ({type(converted_value).__name__})"
                )
                validated_widgets[widget_name] = converted_value
            except ValueError as e:
                logger.error(f"Widget '{widget_name}' conversion failed: {e}")
                errors.append(f"Widget '{widget_name}' conversion failed: {e}")
                continue
        else:
            # Keep as string
            logger.info(f"Widget '{widget_name}' validated with value: '{widget_value}'")
            validated_widgets[widget_name] = widget_value
    
    # Raise exception if any errors found
    if errors:
        error_message = "Widget validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
        logger.error(error_message)
        raise ValueError(error_message)
    
    logger.info("All required widgets validated successfully.")
    return validated_widgets


def validate_available_widgets(
    expected_widgets: List[str],
    widget_types: Optional[Dict[str, type]] = None
) -> Dict[str, Any]:
    """
    Validate and retrieve available Databricks widgets with type conversion.
    
    Args:
        expected_widgets (list[str]): List of expected widget names
        widget_types (dict, optional): Dictionary mapping widget names to target types
    
    Returns:
        dict: A dictionary of available widgets with their converted values
    
    Example:
        >>> widgets = validate_available_widgets(
        ...     expected_widgets=['workspace_path', 'is_pii', 'partition_by'],
        ...     widget_types={
        ...         'is_pii': bool,
        ...         'partition_by': list
        ...     }
        ... )
        >>> print(widgets.get('is_pii'))  # True or None if not provided
    """

    logger.info(f"Validating available widgets from expected list: {expected_widgets}")

    all_widgets = dbutils.widgets.getAll()
    available_widgets = {}

    logger.info(f"All widgets found: {list(all_widgets.keys())}")
    
    for widget_name in expected_widgets:
        # Check if widget exists
        if widget_name in all_widgets:
            widget_value = all_widgets[widget_name]
            
            # Skip empty widgets for available (optional) widgets
            if widget_value is None or (isinstance(widget_value, str) and not widget_value.strip()):
                logger.warning(f"Widget '{widget_name}' is available but empty, skipping.")
                continue
            
            # Convert to target type if specified
            if widget_types and widget_name in widget_types:
                target_type = widget_types[widget_name]
                try:
                    converted_value = _convert_widget_value(widget_value, target_type)
                    logger.info(
                        f"Widget '{widget_name}' converted from '{widget_value}' "
                        f"to {converted_value} ({type(converted_value).__name__})"
                    )
                    available_widgets[widget_name] = converted_value
                except ValueError as e:
                    logger.warning(f"Widget '{widget_name}' conversion failed: {e}, keeping as string")
                    available_widgets[widget_name] = widget_value
            else:
                # Keep as string
                available_widgets[widget_name] = widget_value
                logger.info(f"Widget '{widget_name}' is available with value: '{widget_value}'")
        else:
            logger.warning(f"Widget '{widget_name}' is not available.")
    
    logger.info("Available widgets validation completed.")
    return available_widgets