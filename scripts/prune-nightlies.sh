#!/bin/sh
# Keep the newest N nightly-YYYYMMDD pre-releases and delete the rest
# (and their tags). Never touches v* releases or tags, or any tag that
# is not exactly nightly- followed by eight digits and a pre-release.
#
#   scripts/prune-nightlies.sh [--delete] [--keep N] [--repo OWNER/REPO] [--stdin]
#
# --keep defaults to NIGHTLY_KEEP or 14. Without --delete this is a dry
# run: it prints what it would delete and exits 0. --stdin reads the JSON
# from `gh release list --json tagName,isPrerelease` on stdin instead of
# calling gh (for tests). GH, if set, is the gh binary (or a mock).
set -eu

keep=${NIGHTLY_KEEP:-14}
delete=0
repo=""
stdin=0

usage() {
    sed -n '2,13p' "$0" | sed 's/^# \?//'
    exit 1
}

while [ $# -gt 0 ]; do
    case $1 in
        --delete) delete=1 ;;
        --keep)
            shift
            keep=$1
            ;;
        --repo)
            shift
            repo=$1
            ;;
        --stdin) stdin=1 ;;
        -h|--help) usage ;;
        *)
            echo "unknown argument: $1" >&2
            usage
            ;;
    esac
    shift
done

case $keep in
    ''|*[!0-9]*|0)
        echo "keep must be a positive integer, got: $keep" >&2
        exit 1
        ;;
esac

gh=${GH:-gh}
if [ "$stdin" -eq 1 ]; then
    json=$(cat)
else
    list_args="release list --limit 200 --json tagName,isPrerelease"
    if [ -n "$repo" ]; then
        list_args="$list_args --repo $repo"
    fi
    # shellcheck disable=SC2086
    json=$($gh $list_args)
fi

# Newest first by tag (nightly-YYYYMMDD sorts in date order).
nightlies=$(printf '%s\n' "$json" | jq -r '
    [ .[]
        | select(.isPrerelease == true)
        | select(.tagName | test("^nightly-[0-9]{8}$"))
    ]
    | sort_by(.tagName)
    | reverse
    | .[].tagName
')

n=0
if [ -n "$nightlies" ]; then
    n=$(printf '%s\n' "$nightlies" | wc -l)
fi
echo "found $n nightly pre-release(s); keeping the $keep newest"

if [ -z "$nightlies" ]; then
    exit 0
fi

# A here-doc (not a pipe) so the loop is not a subshell and the counts
# at the end are visible.
i=0
deleted=0
while IFS= read -r tag; do
    i=$((i + 1))
    case $tag in
        nightly-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;;
        *)
            echo "refusing to touch unexpected tag: $tag" >&2
            exit 1
            ;;
    esac
    case $tag in
        v*)
            echo "refusing to touch v* tag: $tag" >&2
            exit 1
            ;;
    esac
    if [ "$i" -le "$keep" ]; then
        echo "keep $tag"
        continue
    fi
    if [ "$delete" -eq 1 ]; then
        del_args="release delete $tag --cleanup-tag --yes"
        if [ -n "$repo" ]; then
            del_args="$del_args --repo $repo"
        fi
        echo "delete $tag"
        # shellcheck disable=SC2086
        $gh $del_args
    else
        echo "would delete $tag"
    fi
    deleted=$((deleted + 1))
done <<NIGHTLIES
$nightlies
NIGHTLIES

if [ "$delete" -eq 1 ]; then
    echo "deleted $deleted old nightly pre-release(s)"
else
    echo "would delete $deleted old nightly pre-release(s) (dry run)"
fi
