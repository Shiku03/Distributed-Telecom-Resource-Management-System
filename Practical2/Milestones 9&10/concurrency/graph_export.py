"""
Render the resource-allocation graph (Pi -> Rj request edges, Rj -> Pi
allocation edges) that Milestone 6 explicitly asks for, in three forms:
a Python dict, a Graphviz DOT file, and an ASCII dump for the terminal.
"""
import json


def to_dict(lock_manager):
    g = lock_manager.resource_allocation_graph()
    return {
        "processes": sorted({p for _, p in g["allocation"]} | {p for p, _ in g["request"]}),
        "resources": sorted({r for r, _ in g["allocation"]} | {r for _, r in g["request"]}),
        "allocation_edges": [{"from": r, "to": p} for r, p in g["allocation"]],
        "request_edges": [{"from": p, "to": r} for p, r in g["request"]],
    }


def to_dot(lock_manager) -> str:
    g = to_dict(lock_manager)
    lines = ["digraph ResourceAllocationGraph {", '  rankdir=LR;',
             '  node [shape=circle]; ' + " ".join(f'"{p}"' for p in g["processes"]) + ";",
             '  node [shape=box];   ' + " ".join(f'"{r}"' for r in g["resources"]) + ";"]
    for e in g["allocation_edges"]:
        lines.append(f'  "{e["from"]}" -> "{e["to"]}" [color=blue, label="holds"];')
    for e in g["request_edges"]:
        lines.append(f'  "{e["from"]}" -> "{e["to"]}" [color=red, style=dashed, label="waits for"];')
    lines.append("}")
    return "\n".join(lines)


def to_ascii(lock_manager) -> str:
    g = to_dict(lock_manager)
    lines = ["Resource-allocation graph:"]
    for e in g["allocation_edges"]:
        lines.append(f'  [{e["from"]}] --holds--> ({e["to"]})')
    for e in g["request_edges"]:
        lines.append(f'  ({e["from"]}) --waits for--> [{e["to"]}]')
    if not g["allocation_edges"] and not g["request_edges"]:
        lines.append("  (empty)")
    return "\n".join(lines)


def write(lock_manager, out_prefix):
    with open(out_prefix + ".json", "w") as f:
        json.dump(to_dict(lock_manager), f, indent=2)
    with open(out_prefix + ".dot", "w") as f:
        f.write(to_dot(lock_manager))
