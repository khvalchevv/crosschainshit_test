"""Parse current bot run log and report alert / broadcast / cycle statistics."""
import re
import sys
from collections import Counter
from pathlib import Path

LOG = Path(r"C:\Users\User\AppData\Local\Temp\claude\C--Users-User\797ea9fd-1fc7-4396-b55a-1ac90d5b7cc0\tasks\bkar655fo.output")

if not LOG.exists():
    sys.exit(f"log not found: {LOG}")

text = LOG.read_text(encoding="utf-8", errors="ignore")

opps_re = re.compile(
    r"summary=([^|]+) \| BUY (\w+) @ \$([\de.+-]+) -> SELL (\w+) @ \$([\de.+-]+) \| gross=([\d.]+)%"
)
opps = []
for m in opps_re.finditer(text):
    opps.append({
        "symbol":   m.group(1).strip(),
        "cheap":    m.group(2),
        "exp":      m.group(4),
        "spread":   float(m.group(6)),
    })

bc_re = re.compile(r"alerter\.broadcast_complete\s+cg_id=(\S+).*?failed=(\d+).*?sent=(\d+)")
broadcasts = [(m.group(1), int(m.group(2)), int(m.group(3))) for m in bc_re.finditer(text)]

stale_killed = len(re.findall(r"verify_killed_stale", text))
liq_killed   = len(re.findall(r"killed_low_liq_or_vol", text))
mon_cycles   = len(re.findall(r"cc_monitor\.cycle_done", text))
det_cycles   = len(re.findall(r"cc_detector\.cycle_done", text))

cycle_re = re.compile(r"cc_monitor\.cycle_done.*?elapsed=([\d.]+)")
elapsed_list = [float(m.group(1)) for m in cycle_re.finditer(text)]

print("=" * 60)
print(" RUN STATS")
print("=" * 60)
print(f"  Monitor cycles:        {mon_cycles}")
print(f"  Detector cycles:       {det_cycles}")
if elapsed_list:
    n = len(elapsed_list)
    avg = sum(elapsed_list) / n
    med = sorted(elapsed_list)[n // 2]
    mn  = min(elapsed_list)
    mx  = max(elapsed_list)
    print(f"  Cycle time:  avg={avg:.1f}s  median={med:.1f}s  min={mn:.1f}s  max={mx:.1f}s")
print()
print(f"  Opportunities triggered:   {len(opps)}")
print(f"  Broadcasts complete:       {len(broadcasts)}")
sent = sum(b[2] for b in broadcasts)
fail = sum(b[1] for b in broadcasts)
print(f"    total messages sent:     {sent}")
print(f"    total send failures:     {fail}")
print()
print(f"  verify_killed_stale:       {stale_killed}")
print(f"  liq/vol gate killed:       {liq_killed}")

if opps:
    print()
    print("=" * 60)
    print(" SPREAD STATS")
    print("=" * 60)
    spreads = sorted(o["spread"] for o in opps)
    avg = sum(spreads) / len(spreads)
    med = spreads[len(spreads) // 2]
    print(f"  avg = {avg:.2f}%   median = {med:.2f}%")
    print(f"  min = {min(spreads):.2f}%   max = {max(spreads):.2f}%")
    print()
    print("  Buckets (5%):")
    buckets = Counter(int(s // 5) * 5 for s in spreads)
    for low in sorted(buckets):
        print(f"    {low:>3}-{low+5}%   x{buckets[low]}")

    print()
    print("  Top symbols:")
    sym_counter = Counter(o["symbol"] for o in opps)
    for sym, cnt in sym_counter.most_common(12):
        sym_spreads = [o["spread"] for o in opps if o["symbol"] == sym]
        sa = sum(sym_spreads) / len(sym_spreads)
        print(f"    {sym:<32} x{cnt:>2}   avg {sa:.2f}%")

    print()
    print("  Top chain pairs:")
    pair_counter = Counter(f"{o['cheap']:<10} -> {o['exp']}" for o in opps)
    for pair, cnt in pair_counter.most_common(10):
        print(f"    {pair:<25} x{cnt}")
