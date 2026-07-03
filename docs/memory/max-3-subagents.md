---
name: max-3-subagents
description: Hard cap on concurrently running subagents — currently 4 (raised from 3)
metadata: 
  node_type: memory
  type: feedback
  originSessionId: e861d2c5-f32d-4d4a-853a-980ee68976d1
---

Never exceed the user's cap on concurrently running subagents (Agent tool / Workflow fan-out). **Current cap = 4** (raised from 3 by the user on 2026-07-03, same day they first set it).

**Why:** User's explicit constraint — cost/rate/oversight control. They've restated it more than once, so honor it precisely.

**How to apply:** When fanning out research or implementation agents, batch so at most 4 are in flight at once; queue the rest and launch as slots free. Applies to Workflow parallel()/pipeline() concurrency too — keep effective concurrency ≤ 4. If the user changes the number again, update this memory.
