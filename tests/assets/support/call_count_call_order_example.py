call_counts[type(state_store)] = {
    '__init__': 1,
    'startup': 1,
    'run': 1,
    'load_all_contexts': sum(minion_started_counts.values()),
    'save_context':
        sum(
            len(m_cls._mn_workflow_spec) * minion_started_counts[m_cls] # type: ignore
            for m_cls in unique_minion_classes.values()
        ),
    'delete_context': sum(minion_started_counts.values()),
    'shutdown': 1
}
call_orders[type(state_store)] = (
    '__init__',
    'startup',
    'run',
    'load_all_contexts',
    'save_context',
    'delete_context',
    'shutdown',
)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

call_counts[type(state_store)] = {
    '__init__': 1,
    'startup': 1,
    'run': 1,
    'load_all_contexts': sum(minion_started_counts.values()),
    'save_context':
        sum(
            len(m_cls._mn_workflow_spec) * minion_started_counts[m_cls] # type: ignore
            for m_cls in unique_minion_classes.values()
        ),
    'delete_context': sum(minion_started_counts.values()),
    'shutdown': 1
}
call_orders[type(state_store)] = tuple(call_counts[type(state_store)].keys())