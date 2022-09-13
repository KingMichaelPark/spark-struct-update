from typing import Callable, List, Optional

from pyspark.sql import functions as F


def struct_repl(col: F.Column, remaining: str, *args, **kwargs) -> F.Column:
    """
    Replace a field within a struct that is contained within an array field.

    If a value you wish to update exists multiple times in len(n) array field
    and you wish to update the value each time, use this in conjunction
    with the array_path argument in the update_struct function.

    Args:
        col (F.Column): The column to update
        remaining (str): dot notated path to traverse and update
        *args (The types to convert)
        **kwargs

    Returns:
        col (F.Column)

    """
    _split = remaining.split(".")
    col_name = _split[0]
    remaining_fields = ".".join(_split[1:])
    return col.withField(
        col_name,
        update_struct(
            col.getField(col_name),
            remaining_fields,
            group_to_primitive,
            None,
            *args,
            **kwargs,
        ),
    )


def group_to_primitive(col: F.Column, remaining: str, *types: List[str]) -> F.Column:
    """
    Replace a group/struct col of two types to a single primitive value.

    This function takes in a column that contains a struct of two type
    values. i.e.

    Example:
        "col": {"field_name": {"value": {"type_1": "type_1", "type_2": "type_2"}}}
        ->
        "col": {"field_name": {"value": "type_2"}}

    This happens in Athena when two types are inferred by the glue crawler and cannot
    be matched with the schema if the schema has migrated between crawls.

    Args:
        col (F.Column): The column that contains a struct of the value to change
        remaining (str): The remaining chain of field names to update from
        Group to Primitive. Not used
        types (List[str]): A list of types to coalesce, casting to the first type
                           provided.

    Returns:
        col (F.Column)

    """

    return F.lit(F.coalesce(*[col.getField(t) for t in types])).cast(types[0])


def update_struct(
    col: F.Column,
    remaining: str,
    struct_func: Callable,
    array_path: Optional[str] = None,
    *args,
    **kwargs
) -> F.Column:
    """
    Traverse a struct field and change a value deeply nested within it.

    In order to traverse a struct field you need to update every value
    from the bottom all the way back up to the top of the structure.

    Args:
        col (F.Column): The top level StructField to traverse
        remaining (str): dot joined path to traverse recursively
        struct_func (Callable): The function to apply on the value you want to update
        array_path (str): A second path that can be traversed if there is an
            array field nested somewhere in the overall structure
        *args
        **kwargs


    Returns:
        col: F.Column

    """
    field_name = remaining.split(".")[0]
    _remaining = remaining.split(".")[1:]
    # Traverse struct recursively
    if len(_remaining) > 0 and len(field_name) > 0:
        remaining = ".".join(_remaining)
        return col.withField(
            field_name,
            update_struct(
                col.getField(field_name),
                remaining,
                struct_func,
                array_path,
                *args,
                **kwargs,
            ),
        )
    # Last traversal before function application
    elif len(_remaining) == 0 and len(field_name) > 0 and array_path:
        return col.withField(
            field_name,
            update_struct(
                col.getField(field_name),
                "",
                struct_func,
                array_path,
                *args,
                **kwargs,
            ),
        )
    else:
        # Array Handling
        if array_path is not None:
            a_field_name = array_path.split(".")[0]
            _a_remaining = array_path.split(".")[1:]
            a_remaining = ".".join(_a_remaining)
            return col.withField(
                a_field_name,
                F.transform(
                    col.getField(a_field_name),
                    lambda c: struct_func(c, a_remaining, *args, **kwargs),
                ),
            )
        # Single Value Handling
        else:
            return col.withField(
                field_name,
                struct_func(col.getField(field_name), field_name, *args, **kwargs),
            )
