# Spark Struct Field Update

Some functions to update nested StructFields in pyspark

## Caveats

This is a python udf which is effectively treated as a black box
according to spark. This means that these calls are going to be much
more performance heavy that a native spark function implementation.

That said there is no way I can find to easily update fields in nested 
spark struct fields.

i.e.

```yaml
top_field:
    nested_field:
       even_more_nested_field:
           value: to_update
```

This should also work for arrays in the struct as well:

```yaml
top_field:
    nested_field:
        array_field:
            - struct_1
            - struct_2
            - struct_3
 ```
 
 PR's welcome
