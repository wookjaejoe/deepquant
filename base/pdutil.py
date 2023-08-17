def sort_columns(
    columns: list[str],
    forward: list[str] = None,
    backward: list[str] = None
):
    forward = [] if forward is None else forward
    backward = [] if backward is None else backward
    return forward + [c for c in columns if c not in forward + backward] + backward
