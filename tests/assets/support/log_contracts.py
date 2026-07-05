from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

_LOG_IDENTITY_KEYS_ORCHESTRATION = frozenset({"orchestration_id"})
_LOG_IDENTITY_KEYS_MINION = frozenset(
    {
        "minion_id",
        "minion_instance_id",
        "minion_config_id",
        "minion_module_path",
    }
)
_LOG_IDENTITY_KEYS_PIPELINE = frozenset({"pipeline_id", "pipeline_module_path"})
_LOG_IDENTITY_KEYS_RESOURCE = frozenset({"resource_id", "resource_module_path"})
_LOG_IDENTITY_KEYS_DOMAIN = (
    _LOG_IDENTITY_KEYS_ORCHESTRATION
    | _LOG_IDENTITY_KEYS_PIPELINE
    | _LOG_IDENTITY_KEYS_MINION
    | _LOG_IDENTITY_KEYS_RESOURCE
)

_LOG_CONTEXT_KEYS_RESOURCE_CALL = frozenset({"resource_method"})


@dataclass(frozen=True)
class _LogContract:
    name: str
    required_keys: frozenset[str] = frozenset()
    forbidden_keys: frozenset[str] = frozenset()

    def matches(self, kwargs: Mapping[str, object]) -> bool:
        keys = frozenset(kwargs)
        return self.required_keys <= keys and not (self.forbidden_keys & keys)


_LOG_CONTRACTS: Sequence[_LogContract] = (
    _LogContract(
        name="generic",
        forbidden_keys=_LOG_IDENTITY_KEYS_DOMAIN,
    ),
    _LogContract(
        name="orchestration_identity",
        required_keys=_LOG_IDENTITY_KEYS_ORCHESTRATION,
        forbidden_keys=_LOG_IDENTITY_KEYS_DOMAIN - _LOG_IDENTITY_KEYS_ORCHESTRATION,
    ),
    _LogContract(
        name="minion_identity",
        required_keys=_LOG_IDENTITY_KEYS_MINION,
        forbidden_keys=_LOG_IDENTITY_KEYS_DOMAIN - _LOG_IDENTITY_KEYS_MINION,
    ),
    _LogContract(
        name="minion_orchestration",
        # Minion workflow logs currently carry bare pipeline_id because Minion
        # receives only partial orchestration context from Gru. Keep this inline
        # so it does not look like a first-class pipeline reference contract.
        required_keys=(
            _LOG_IDENTITY_KEYS_MINION
            | _LOG_IDENTITY_KEYS_ORCHESTRATION
            | frozenset({"pipeline_id"})
        ),
        forbidden_keys=(
            _LOG_IDENTITY_KEYS_DOMAIN
            - _LOG_IDENTITY_KEYS_MINION
            - _LOG_IDENTITY_KEYS_ORCHESTRATION
            - frozenset({"pipeline_id"})
        ),
    ),
    _LogContract(
        name="orchestration_pipeline_identity",
        required_keys=(
            _LOG_IDENTITY_KEYS_ORCHESTRATION
            | _LOG_IDENTITY_KEYS_PIPELINE
            | _LOG_IDENTITY_KEYS_MINION
        ),
        forbidden_keys=(
            _LOG_IDENTITY_KEYS_DOMAIN
            - _LOG_IDENTITY_KEYS_ORCHESTRATION
            - _LOG_IDENTITY_KEYS_PIPELINE
            - _LOG_IDENTITY_KEYS_MINION
        ),
    ),
    _LogContract(
        name="orchestration_pipeline_resource_identity",
        required_keys=_LOG_IDENTITY_KEYS_DOMAIN,
    ),
    _LogContract(
        name="pipeline_identity",
        required_keys=_LOG_IDENTITY_KEYS_PIPELINE,
        forbidden_keys=_LOG_IDENTITY_KEYS_DOMAIN - _LOG_IDENTITY_KEYS_PIPELINE,
    ),
    _LogContract(
        name="minion_pipeline_identity",
        required_keys=_LOG_IDENTITY_KEYS_MINION | _LOG_IDENTITY_KEYS_PIPELINE,
        forbidden_keys=(
            _LOG_IDENTITY_KEYS_DOMAIN
            - _LOG_IDENTITY_KEYS_MINION
            - _LOG_IDENTITY_KEYS_PIPELINE
        ),
    ),
    _LogContract(
        name="resource_identity",
        required_keys=_LOG_IDENTITY_KEYS_RESOURCE,
        forbidden_keys=(
            (_LOG_IDENTITY_KEYS_DOMAIN - _LOG_IDENTITY_KEYS_RESOURCE)
            | _LOG_CONTEXT_KEYS_RESOURCE_CALL
        ),
    ),
    _LogContract(
        name="resource_call",
        required_keys=_LOG_IDENTITY_KEYS_RESOURCE | _LOG_CONTEXT_KEYS_RESOURCE_CALL,
        forbidden_keys=_LOG_IDENTITY_KEYS_DOMAIN - _LOG_IDENTITY_KEYS_RESOURCE,
    ),
)


@dataclass(frozen=True)
class _LogContractViolation:
    kind: Literal["no_match", "multiple_matches"]
    msg: str
    kwargs: Mapping[str, object]
    matching_contract_names: tuple[str, ...]


def _find_log_contract_violations(logs: Sequence[Any]) -> tuple[_LogContractViolation, ...]:
    violations: list[_LogContractViolation] = []
    for log in logs:
        matching_contract_names = tuple(
            contract.name for contract in _LOG_CONTRACTS if contract.matches(log.kwargs)
        )
        if len(matching_contract_names) == 1:
            continue
        violations.append(
            _LogContractViolation(
                kind=(
                    "no_match"
                    if not matching_contract_names
                    else "multiple_matches"
                ),
                msg=log.msg,
                kwargs=log.kwargs,
                matching_contract_names=matching_contract_names,
            )
        )
    return tuple(violations)


def assert_each_log_matches_exactly_one_contract(logs: Sequence[Any]) -> None:
    violations = _find_log_contract_violations(logs)
    if not violations:
        return

    details = "\n".join(
        (
            f"{violation.msg}: kind={violation.kind!r} "
            f"matched={violation.matching_contract_names!r} "
            f"kwargs={dict(violation.kwargs)!r}"
        )
        for violation in violations
    )
    raise AssertionError(f"log contract violations:\n{details}")
