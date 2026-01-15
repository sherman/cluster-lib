# Rebalancing Threshold Guide

## What the threshold does
- It is the stop condition in the rebalance loop.
- If the gap between the heaviest and lightest node weight is **below** the threshold, rebalancing stops.
- If the gap stays **above** the threshold, the balancer keeps trying relocations.

## How weights move
- A single relocation changes weight by roughly:
  - `θ_shard` when shard count changes,
  - `θ_index` if index count changes,
  - plus ingest/disk contributions when those inputs differ.
- With equal factors (all 4 set), `θ ≈ 0.25` for each term.

## Picking a value
- **Avoid oscillation:** set the threshold **above** the post-move gap you expect in steady state. For two nodes and only shard factor, a threshold > `1 * θ_shard` stops odd-count ping-pong.
- **Allow one-shard moves:** set the threshold just **below** the delta of a single useful move (e.g., ~`0.9 * θ_shard` if only shard factor matters).
- **Mixed factors:** estimate the smallest expected delta per move (`θ_shard ± θ_index` plus load terms) and place the threshold slightly below (to keep moving) or above (to stop) that value.

## Quick starting points
- Shard-only (`θ_shard = 1`): use ~`1.1` to prevent odd-count oscillation; use ~`0.9` to allow one-shard balancing.
- All factors equal (`θ ≈ 0.25` each): start around `0.3–0.4`.
- Fast but less strict rebalance: raise the threshold.
- Stricter balance at the cost of more moves: lower the threshold (but keep it > 0 to avoid loops).
