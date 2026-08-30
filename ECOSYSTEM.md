# XO ecosystem map

## Verdict

There is one XO source lineage and several consumers, mirrors, and archives. No independently evolved XO rewrite was found.

- **Canonical historical repository:** `/Users/magic/wholesomegarden/xo-benedict`
- **Feature-complete presentation line:** branch `xo1`, commit `101a190` (2024-02-26), containing the XO-JS demo and vision/specification documents.
- **Newest Benedict-derived core line:** branch `xo2`, commit `149b171` (2024-06-04), whose `xo.py` hash matches the later embedded MagicLLight copy after package-relative import adaptation.
- **Newest materially extended integration:** MagicLLight's embedded `freshServer.py` (2024-06-22), adding configurable ports and generator streaming on top of the same core.
- **New authoritative unification project:** `/Users/magic/Creations/XO`.

## Lineage

```text
python-benedict upstream
        │
        └── fire17/xo-benedict
             ├── main      upstream-shaped baseline
             ├── xo2       newest Benedict-derived core (2024-06-04)
             └── xo1       complete XO vision, Branch, RPC, JS demo (2024-02-26)
                    │
                    ├── AAA consumer
                    ├── MagicLight / AIrouter consumer
                    │     └── later FreshZero/FreshClient generator-RPC changes
                    ├── XO-Svelte demo (inside xo-benedict; not a separate project)
                    ├── CLEANUP_DEVENV snapshots
                    └── AirDrop mirrors
```

The unified implementation does **not** select one directory and copy it wholesale. It takes semantic evidence from `xo1`, the latest core corrections from `xo2`, and the later RPC integration from MagicLight. It replaces their entanglement with a purpose-built package.

## Projects and copies

| Path | Classification | Material XO role | Authority |
|---|---|---|---|
| `/Users/magic/wholesomegarden/xo-benedict` | canonical historical source | complete experiment, Git history, vision, demos | primary behavioral evidence |
| `/Users/magic/wholesomegarden/AAA` | vertical consumer | agent state, `xoBranch`, Redis, `FreshZero` | consumer contract evidence |
| `/Users/magic/wholesomegarden/AAA/xo_benedict` | vendored mirror | embedded XO package | non-authoritative |
| `/Users/magic/wholesomegarden/magicllight/transparent-web-app` | vertical consumer | realtime application/UI integration | consumer contract evidence |
| `/Users/magic/wholesomegarden/magicllight/transparent-web-app/xo_benedict` | vendored mirror | XO-Svelte/Python bridge | non-authoritative |
| `/Users/magic/wholesomegarden/magicllight/magicllight/core/airouter/pipelines/xo_benedict` | evolved embedded integration | same xo2 core plus newer streaming RPC | secondary implementation evidence |
| `/Users/magic/wholesomegarden/CLEANUP_DEVENV/XO/xo-benedict` | archive | complete snapshot | provenance only |
| `/Users/magic/wholesomegarden/CLEANUP_DEVENV/XO/AAA/xo_benedict` | archive | AAA snapshot | provenance only |
| `/Users/magic/wholesomegarden/CLEANUP_DEVENV/MAGIC_UI/MAGICLIGHT/magicllight/core/airouter/pipelines/xo_benedict` | archive | AIrouter snapshot | provenance only |
| `/Users/magic/Downloads/Airdroped from Omer's Macbook Air/magic/wholesomegarden/xo-benedict` | external mirror | transferred full snapshot | provenance only |
| `/Users/magic/Downloads/Airdroped from Omer's Macbook Air/magic/wholesomegarden/magicllight/transparwnt-web-app/xo_benedict` | incomplete external mirror | partial transferred copy | provenance only |

## Feature provenance

| Capability | Strongest evidence |
|---|---|
| value and descendants coexist | `xo-benedict/VISION.MD:3-9`, `xo.py` |
| dynamic attribute/keypath expansion | `xo.py:1442-1462` |
| mutation hooks and subscriptions | `xo.py:1464-1513` and `VISION.MD:123-147` |
| Redis autosave and realtime process sync | `xo.py:1592-2293`, commits `f46ab2b`, `1c9b839`, `a81eb05` |
| revisioned/branching state | `xoDeque.py`, `VISION.MD:53-59`, commits `d2e1b14` through `a81eb05` |
| function exposure and dynamic RPC proxy | `freshServer.py`, `freshClient.py`, recovered Codex session examples |
| generator/streaming RPC | MagicLight embedded `freshServer.py:89-177`, `freshClient.py:40-63` |
| Python-to-JavaScript realtime sync | `JS.py`, `freshSvelt/`, commits `c7c2d23`, `101a190` |
| agent application | `AAA/AAA.py`, `AAA/main.py` |
| multimodal/UI application | MagicLight/AIrouter consumers and recovered run instructions |

## Sessions

The available session corpus contains one primary XO recovery/operation conversation:

| Session | Harness | Role |
|---|---|---|
| `019c3cff-761a-7362-b91f-664059e56860` — “Organize old terminal projects” | Codex | primary human-authored project map, run instructions, RPC and XO-Svelte examples |
| `019c3f91-2bff-7da3-9825-12a76ab4e8d7` — “Create showcase site from report” | Codex | secondary showcase using prior cleanup report; no XO implementation authorship |

No historical Claude implementation session was found. Recent Claude concept matches refer to newer ideas such as live application insertion or DXOS and are not evidence of the 2024 XO implementation. The implementation’s authoritative authorship timeline is Git: 2024-02-11 through 2024-06-04, with the MagicLight embedded integration modified through 2024-06-22.

Machine-readable session evidence is preserved in `.deify/session-inventory.json`; verbatim human instructions are in `origins.md`.

## Migration implication

AAA and MagicLight should eventually depend on one released XO package rather than carry nested repositories. That migration happens only after behavioral compatibility scenarios pass. Archives and AirDrop mirrors remain read-only provenance; they are not deletion candidates during this mission.
