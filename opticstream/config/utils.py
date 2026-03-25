from pydantic import BaseModel


def with_positions(cls: type[BaseModel]) -> type[BaseModel]:
    cls.model_rebuild()
    for i, (name, field) in enumerate(cls.model_fields.items()):
        extra = dict(field.json_schema_extra or {})
        extra["position"] = i
        field.json_schema_extra = extra
    return cls
