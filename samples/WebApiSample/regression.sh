#!/usr/bin/env bash
# Regression test for WebApiSample endpoints.
# Usage: ./regression.sh [base_url]   (default: http://localhost:5299)

BASE="${1:-http://localhost:5299}"
PASS=0; FAIL=0

# ── helpers ──────────────────────────────────────────────────────────────────

check() {
  local name="$1" url="$2" expect_key="${3:-}"
  local status body
  body=$(curl -sf --max-time 45 "$url" 2>/dev/null)
  status=$(curl -so /dev/null -w "%{http_code}" --max-time 45 "$url" 2>/dev/null)

  if [[ "$status" != "200" ]]; then
    echo "FAIL  [$name] HTTP $status"
    FAIL=$((FAIL+1)); return
  fi

  if ! echo "$body" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    echo "FAIL  [$name] invalid JSON (first 150: ${body:0:150})"
    FAIL=$((FAIL+1)); return
  fi

  if [[ -n "$expect_key" ]]; then
    local ok
    ok=$(echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
if not isinstance(d, dict) or '$expect_key' not in d:
    print('missing')
    sys.exit(1)
print('ok')
" 2>/dev/null) || ok="error"
    if [[ "$ok" != "ok" ]]; then
      local keys
      keys=$(echo "$body" | python3 -c "import sys,json; d=json.load(sys.stdin); print(list(d.keys()) if isinstance(d,dict) else type(d).__name__)" 2>/dev/null || echo "?")
      echo "FAIL  [$name] missing key '$expect_key', got: $keys"
      FAIL=$((FAIL+1)); return
    fi
  fi

  echo "PASS  [$name]${expect_key:+ key='$expect_key'}"
  PASS=$((PASS+1))
}

check_array() {
  local name="$1" url="$2" min_items="${3:-1}"
  local status body
  body=$(curl -sf --max-time 45 "$url" 2>/dev/null)
  status=$(curl -so /dev/null -w "%{http_code}" --max-time 45 "$url" 2>/dev/null)

  if [[ "$status" != "200" ]]; then
    echo "FAIL  [$name] HTTP $status"
    FAIL=$((FAIL+1)); return
  fi

  local n
  n=$(echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert isinstance(d, list), f'expected list, got {type(d).__name__}'
print(len(d))
" 2>/dev/null)

  if [[ -z "$n" ]]; then
    echo "FAIL  [$name] expected JSON array (first 150: ${body:0:150})"
    FAIL=$((FAIL+1)); return
  fi

  if (( n < min_items )); then
    echo "FAIL  [$name] array has $n items, expected >= $min_items"
    FAIL=$((FAIL+1)); return
  fi

  echo "PASS  [$name] array[$n]"
  PASS=$((PASS+1))
}

check_ndjson() {
  local name="$1" url="$2" min_lines="${3:-1}"
  local status body
  body=$(curl -sf --max-time 45 "$url" 2>/dev/null)
  status=$(curl -so /dev/null -w "%{http_code}" --max-time 45 "$url" 2>/dev/null)

  if [[ "$status" != "200" ]]; then
    echo "FAIL  [$name] HTTP $status"
    FAIL=$((FAIL+1)); return
  fi

  local ok=true line_count=0 bad_line=""
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    if ! echo "$line" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
      ok=false; bad_line="$line"; break
    fi
    line_count=$((line_count+1))
  done <<< "$body"

  if [[ "$ok" != "true" ]]; then
    echo "FAIL  [$name] invalid NDJSON line: ${bad_line:0:120}"
    FAIL=$((FAIL+1)); return
  fi

  if (( line_count < min_lines )); then
    echo "FAIL  [$name] only $line_count valid NDJSON lines, expected >= $min_lines"
    FAIL=$((FAIL+1)); return
  fi

  echo "PASS  [$name] NDJSON[$line_count lines]"
  PASS=$((PASS+1))
}

# ── tests ─────────────────────────────────────────────────────────────────────

echo "=== WebApiSample regression — $BASE ==="
echo ""

check_array "framer/array"    "$BASE/framer/array?limit=3"     3
check       "framer/envelope" "$BASE/framer/envelope?limit=3"  "results"

check "level1/passthrough" "$BASE/level1/passthrough?limit=3" "results"
check "level1/transform"   "$BASE/level1/transform?limit=3"   "results"
check "level1/filter"      "$BASE/level1/filter?albumId=1"    "photos"

check "level2/typed" "$BASE/level2/typed?limit=3" "products"
check "level3/manual" "$BASE/level3/manual"        "results"
check "level4/aggregate" "$BASE/level4/aggregate"  "totalValue"

check_ndjson "ndjson/products"       "$BASE/ndjson/products?limit=3"       3
check_ndjson "ndjson/comments"       "$BASE/ndjson/comments"               3
check_ndjson "ndjson/product-titles" "$BASE/ndjson/product-titles?limit=3" 3

check "deep/select-many" "$BASE/deep/select-many" "todos"
check "deep/nested"      "$BASE/deep/nested"       "items"
check "deep/jsonpath"    "$BASE/deep/jsonpath"     "products"
check "multi-source"     "$BASE/multi-source"      "todos"

echo ""
echo "=== $PASS passed, $FAIL failed ==="
[[ $FAIL -eq 0 ]]
