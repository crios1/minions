"""Shared AST inspection for component identity CLI tools."""

from __future__ import annotations

import ast

DECORATOR_BY_KIND = {
    "minion": "minion_id",
    "pipeline": "pipeline_id",
    "resource": "resource_id",
}
_BASE_KIND_BY_NAME = {
    "Minion": "minion",
    "Pipeline": "pipeline",
    "Resource": "resource",
}


def _name_from_expr(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return _name_from_expr(node.value)
    return None


def decorator_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Call):
        return _name_from_expr(node.func)
    return _name_from_expr(node)


def component_kind(node: ast.ClassDef) -> str | None:
    kinds = {
        _BASE_KIND_BY_NAME[base_name]
        for base in node.bases
        if (base_name := _name_from_expr(base)) in _BASE_KIND_BY_NAME
    }
    if len(kinds) == 1:
        return next(iter(kinds))
    return None


def has_component_id_decorator(node: ast.ClassDef, kind: str) -> bool:
    expected = DECORATOR_BY_KIND[kind]
    return any(decorator_name(decorator) == expected for decorator in node.decorator_list)


def module_string_constants(tree: ast.Module) -> dict[str, str]:
    constants: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        value = node.value
        if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if isinstance(target, ast.Name):
                constants[target.id] = value.value
    return constants


def component_id_for_class(
    node: ast.ClassDef,
    kind: str,
    constants: dict[str, str],
) -> tuple[str | None, int | None]:
    expected = DECORATOR_BY_KIND[kind]
    for decorator in node.decorator_list:
        if decorator_name(decorator) != expected:
            continue
        if not isinstance(decorator, ast.Call) or not decorator.args:
            return None, decorator.lineno
        arg = decorator.args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value, decorator.lineno
        if isinstance(arg, ast.Name):
            return constants.get(arg.id), decorator.lineno
        return None, decorator.lineno
    return None, None
