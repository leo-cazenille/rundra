from rundra.cli.render import _human_sequence


def test_human_sequence_compacts_large_collections_with_total() -> None:
    rendered = _human_sequence(tuple(range(1000)))

    assert rendered.startswith("0, 1, 2, 3, 4, 5, 6, 7")
    assert "990 omitted" in rendered
    assert rendered.endswith("998, 999 (1000 total)")


def test_human_sequence_keeps_small_collections_exact() -> None:
    assert _human_sequence((2, 4, 6), separator=",") == "2,4,6"
