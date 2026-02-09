from tests.support.gru_scenario.directives import (
    Concurrent,
    GruShutdown,
    MinionStart,
    MinionStop,
    WaitWorkflows,
    iter_directives_flat,
)


def test_iter_directives_flattens_concurrent():
    d1a = MinionStart(minion="m1", pipeline="p1")
    d1b = MinionStart(minion="m2", pipeline="p2")
    d2 = WaitWorkflows()
    d3a = MinionStop(expect_success=True, name_or_instance_id="m1")
    d3b = MinionStop(expect_success=True, name_or_instance_id="m2")
    d4 = GruShutdown()

    directives = [Concurrent(d1a, d1b), d2, Concurrent(d3a, d3b), d4]

    assert list(iter_directives_flat(directives)) == [d1a, d1b, d2, d3a, d3b, d4]
