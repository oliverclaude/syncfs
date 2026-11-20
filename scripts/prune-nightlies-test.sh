#!/bin/sh
# Unit tests for prune-nightlies.sh, fed a fake release list on stdin.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
script="$root/scripts/prune-nightlies.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

# 16 dated nightlies, newest last in the input (script must sort), plus
# releases that must never be selected: a v* pre-release, a v* full
# release, a non-prerelease nightly tag, and a nightly- tag that is not
# eight digits.
json=$(cat <<'JSON'
[
  {"tagName": "nightly-20261105", "isPrerelease": true},
  {"tagName": "nightly-20261106", "isPrerelease": true},
  {"tagName": "nightly-20261107", "isPrerelease": true},
  {"tagName": "nightly-20261108", "isPrerelease": true},
  {"tagName": "nightly-20261109", "isPrerelease": true},
  {"tagName": "nightly-20261110", "isPrerelease": true},
  {"tagName": "nightly-20261111", "isPrerelease": true},
  {"tagName": "nightly-20261112", "isPrerelease": true},
  {"tagName": "nightly-20261113", "isPrerelease": true},
  {"tagName": "nightly-20261114", "isPrerelease": true},
  {"tagName": "nightly-20261115", "isPrerelease": true},
  {"tagName": "nightly-20261116", "isPrerelease": true},
  {"tagName": "nightly-20261117", "isPrerelease": true},
  {"tagName": "nightly-20261118", "isPrerelease": true},
  {"tagName": "nightly-20261119", "isPrerelease": true},
  {"tagName": "nightly-20261120", "isPrerelease": true},
  {"tagName": "v0.1.0-pre", "isPrerelease": true},
  {"tagName": "v0.1.0", "isPrerelease": false},
  {"tagName": "nightly-20260101", "isPrerelease": false},
  {"tagName": "nightly-oops", "isPrerelease": true},
  {"tagName": "nightly-2026-11-20", "isPrerelease": true}
]
JSON
)

out=$tmp/out
printf '%s\n' "$json" | "$script" --stdin --keep 14 >"$out"
grep -q "found 16 nightly pre-release(s); keeping the 14 newest" "$out" || fail "summary: $(cat "$out")"
# Newest 14 are 20261107..20261120
for d in 07 08 09 10 11 12 13 14 15 16 17 18 19 20; do
    grep -qx "keep nightly-202611$d" "$out" || fail "should keep nightly-202611$d"
done
grep -qx "would delete nightly-20261106" "$out" || fail "should delete 1106"
grep -qx "would delete nightly-20261105" "$out" || fail "should delete 1105"
grep -q "would delete 2 old nightly pre-release(s) (dry run)" "$out" || fail "delete count"
# Must not mention v* or malformed tags at all as keep/delete targets
if grep -E '^(keep|would delete|delete) ' "$out" | grep -Eq 'v0\.|nightly-oops|nightly-2026-11-20|nightly-20260101'; then
    fail "touched a tag that must be left alone: $(cat "$out")"
fi
echo "ok: dry-run prunes only the two oldest dated nightlies"

# Already at or under the limit: nothing to delete.
printf '%s\n' "$json" | "$script" --stdin --keep 16 >"$out"
grep -q "would delete 0 old nightly pre-release(s) (dry run)" "$out" || fail "keep 16: $(cat "$out")"
if grep -q "would delete nightly-" "$out"; then
    fail "keep 16 should delete nothing: $(cat "$out")"
fi
echo "ok: keep=16 deletes nothing"

# Only a v* pre-release in the list.
printf '%s\n' '[{"tagName":"v0.1.0-pre","isPrerelease":true}]' | "$script" --stdin --keep 14 >"$out"
grep -q "found 0 nightly pre-release(s)" "$out" || fail "v* only: $(cat "$out")"
if grep -q "v0.1.0-pre" "$out"; then
    fail "mentioned the v* tag: $(cat "$out")"
fi
echo "ok: a list of only v* tags is a no-op"

# Empty list.
printf '%s\n' '[]' | "$script" --stdin >"$out"
grep -q "found 0 nightly pre-release(s)" "$out" || fail "empty: $(cat "$out")"
echo "ok: empty list is a no-op"

# Unsorted input still keeps the newest by tag, not list order.
unsorted=$(cat <<'JSON'
[
  {"tagName": "nightly-20261101", "isPrerelease": true},
  {"tagName": "nightly-20261103", "isPrerelease": true},
  {"tagName": "nightly-20261102", "isPrerelease": true}
]
JSON
)
printf '%s\n' "$unsorted" | "$script" --stdin --keep 1 >"$out"
grep -qx "keep nightly-20261103" "$out" || fail "sort keep: $(cat "$out")"
grep -qx "would delete nightly-20261102" "$out" || fail "sort mid: $(cat "$out")"
grep -qx "would delete nightly-20261101" "$out" || fail "sort old: $(cat "$out")"
echo "ok: sorts by tag name, not input order"

# --delete calls gh (mocked) only for the tags beyond N, never for v*.
mock=$tmp/gh
deleted=$tmp/deleted
: >"$deleted"
cat >"$mock" <<'MOCK'
#!/bin/sh
# Record "release delete TAG ..." lines; other commands should not happen.
echo "gh $*" >>"${DELETED_LOG:?}"
echo "$*" | grep -q '^release delete ' || {
    echo "unexpected gh invocation: $*" >&2
    exit 1
}
MOCK
chmod +x "$mock"
printf '%s\n' "$json" | DELETED_LOG=$deleted GH=$mock \
    "$script" --stdin --delete --keep 14 --repo oliverclaude/syncfs >"$out"
grep -qx "delete nightly-20261106" "$out" || fail "delete log 1106: $(cat "$out")"
grep -qx "delete nightly-20261105" "$out" || fail "delete log 1105: $(cat "$out")"
grep -q "deleted 2 old nightly pre-release(s)" "$out" || fail "deleted count: $(cat "$out")"
# Two gh calls, exact args, and no v*.
test "$(wc -l <"$deleted")" -eq 2 || fail "gh call count: $(cat "$deleted")"
grep -qx "gh release delete nightly-20261106 --cleanup-tag --yes --repo oliverclaude/syncfs" "$deleted" || fail "gh args 1106: $(cat "$deleted")"
grep -qx "gh release delete nightly-20261105 --cleanup-tag --yes --repo oliverclaude/syncfs" "$deleted" || fail "gh args 1105: $(cat "$deleted")"
if grep -q 'v0' "$deleted"; then
    fail "gh was asked to delete a v* tag: $(cat "$deleted")"
fi
echo "ok: --delete invokes gh only for the two oldest nightlies"

# Invalid --keep
if "$script" --stdin --keep 0 </dev/null >"$out" 2>"$tmp/err"; then
    fail "keep 0 should fail"
fi
grep -q "keep must be a positive integer" "$tmp/err" || fail "keep 0 message: $(cat "$tmp/err")"
if "$script" --stdin --keep -1 </dev/null >"$out" 2>"$tmp/err"; then
    fail "keep -1 should fail"
fi
echo "ok: rejects a non-positive --keep"

echo "all prune-nightlies tests passed"
