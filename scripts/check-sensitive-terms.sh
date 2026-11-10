#!/usr/bin/env bash
#
# check-sensitive-terms.sh - pre-publish gate for this public repo.
#
# Ported from voitta-ai/skillz (scripts/check-sensitive-terms.sh); the checks
# are generic, only the wordlist path and the default scan targets differ.
#
# Greps the given files/dirs for content that must never be published: key and
# token shapes, cloud account ids, private IPs, internal-domain hostnames.
# voitta-gateway ships example configs, deploy files and pasted terminal
# output, which is exactly the material a real value hides in. Exits non-zero
# on a match, so it can gate CI or a pre-push hook.
#
# The paradox the original design resolves: a denylist of *names* (clients,
# employers, hosts) is itself sensitive and cannot live in a public repo. So
# this script ships only STRUCTURAL patterns, and reads any name-based terms
# from a PRIVATE, out-of-repo wordlist (one term per line; blank lines and
# lines starting with # ignored), at $GATEWAY_SENSITIVE_TERMS_FILE, default
# ~/.config/voitta-gateway/sensitive-terms.txt and then ~/.config/skillz/.
#
# The shared fallback is not tidiness. A wordlist is per MACHINE, not per repo:
# the names that must not be published are the same whichever checkout you are
# standing in. Measured 2026-09-16 in shmobster -- its own path was empty while the
# skillz one held 34 terms, so every run here passed with the name half off
# while the list that would have caught something sat one directory away.
#
# And a run WITHOUT a wordlist no longer reports a bare "clean". It did, on
# stdout, with the explanation on stderr where a caller reading the result does
# not look -- a gate that cannot do its job saying the same word as one that
# did. Set GATEWAY_SENSITIVE_TERMS_REQUIRED=1 to make the absence fatal;
# CI leaves it unset on purpose, because a private wordlist cannot live on a
# public runner and structural-only is the honest best it can do there.
#
# Usage:
#   scripts/check-sensitive-terms.sh <path> [<path> ...]
#   GATEWAY_SENSITIVE_TERMS_FILE=/other/list.txt \
#     scripts/check-sensitive-terms.sh voitta_gateway/ examples/
#
# Exit codes: 0 = clean, 1 = matches found, 2 = usage error, a
# GATEWAY_SENSITIVE_TERMS_FILE that was set but does not exist, or no
# wordlist found at all while GATEWAY_SENSITIVE_TERMS_REQUIRED=1.
#
# bash 3.2 compatible (macOS default); no bashisms beyond 3.2.

set -u

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <path> [<path> ...]" >&2
  exit 2
fi

# Structural patterns - safe to live in the public repo. Extended-regex.
# Each entry: "label|regex".
STRUCTURAL="
aws-account-id|(^|[^0-9])[0-9]{12}([^0-9]|$)
aws-access-key|AKIA[0-9A-Z]{16}
aws-secret-key|(^|[^A-Za-z0-9/+])[A-Za-z0-9/+]{40}([^A-Za-z0-9/+]|$)
slack-bot-token|xox[baprs]-[0-9A-Za-z-]{10,}
slack-app-token|xapp-[0-9]-[0-9A-Za-z-]{10,}
github-token|gh[posru]_[0-9A-Za-z]{30,}
openai-key|(^|[^A-Za-z0-9_-])sk-(proj-)?[A-Za-z0-9]{20,}
google-api-key|AIza[0-9A-Za-z_-]{30,}
private-key-block|-----BEGIN [A-Z ]*PRIVATE KEY-----
private-ip-10|(^|[^0-9])10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}
private-ip-192|(^|[^0-9])192\.168\.[0-9]{1,3}\.[0-9]{1,3}
private-ip-172|(^|[^0-9])172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3}
internal-domain|[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?\.(internal|corp|intranet)\b
"

status=0

# Set to "-i" by the wordlist pass: names are written in every casing
# (Foo, foo, FOO), so a case-sensitive term match misses most of them.
# Structural patterns stay case-SENSITIVE on purpose - AKIA, sk-, xoxb-,
# AIza are fixed-case prefixes, and -i would only add false positives.
CASE_FLAG=""

check_pattern() {
  label="$1"
  regex="$2"
  shift 2
  # grep -rEn over the paths; -I skips binaries. Suppress the "no match" exit.
  matches=$(grep -rEnI $CASE_FLAG "$regex" "$@" 2>/dev/null)
  if [ -n "$matches" ]; then
    echo "SENSITIVE [$label]:" >&2
    echo "$matches" | sed 's/^/  /' >&2
    status=1
  fi
}

# 1) structural patterns
echo "$STRUCTURAL" | while IFS='|' read -r label regex; do
  [ -z "$label" ] && continue
  echo "${label}|${regex}"
done > /tmp/.gateway_structural.$$
# (piping into a while-subshell loses $status in bash 3.2; iterate via a temp file)
while IFS='|' read -r label regex; do
  [ -z "$label" ] && continue
  check_pattern "$label" "$regex" "$@"
done < /tmp/.gateway_structural.$$
rm -f /tmp/.gateway_structural.$$

# 2) optional private wordlist (client/account names etc.)
CONFIG_HOME="${XDG_CONFIG_HOME:-$HOME/.config}"
DEFAULT_TERMS_FILE="$CONFIG_HOME/voitta-gateway/sensitive-terms.txt"
# A wordlist is per machine, not per repo. Prefer this repo's own path, then
# the shared one the sibling repos use.
SHARED_TERMS_FILE="$CONFIG_HOME/skillz/sensitive-terms.txt"
terms_file="${GATEWAY_SENSITIVE_TERMS_FILE:-}"
if [ -z "$terms_file" ]; then
  if [ -f "$DEFAULT_TERMS_FILE" ]; then
    terms_file="$DEFAULT_TERMS_FILE"
  elif [ -f "$SHARED_TERMS_FILE" ]; then
    terms_file="$SHARED_TERMS_FILE"
  else
    terms_file="$DEFAULT_TERMS_FILE"
  fi
fi
wordlist_ran=0

if [ -f "$terms_file" ]; then
  wordlist_ran=1
  # Case-insensitive and WHOLE-WORD (-w): a short listed name otherwise fires
  # inside ordinary words and identifiers ("rubella", "isLabelEvent"), and a
  # gate that cries wolf gets bypassed with --no-verify. A word boundary is any
  # non-[A-Za-z0-9_] character, so "#name", "name-site" and "name.com" still match.
  CASE_FLAG="-i -w"
  while IFS= read -r term; do
    case "$term" in
      ""|\#*) continue ;;
    esac
    # The term is treated as an extended regex.
    check_pattern "private-term" "$term" "$@"
  done < "$terms_file"
  CASE_FLAG=""
  echo "using name wordlist: $terms_file" >&2
elif [ -n "${GATEWAY_SENSITIVE_TERMS_FILE:-}" ]; then
  # Explicitly pointed at a file that isn't there - that is an error, not a
  # silent downgrade to structural-only.
  echo "error: GATEWAY_SENSITIVE_TERMS_FILE=$GATEWAY_SENSITIVE_TERMS_FILE does not exist" >&2
  exit 2
else
  echo "note: no name wordlist - structural checks only." >&2
  echo "      looked at $DEFAULT_TERMS_FILE" >&2
  echo "           then $SHARED_TERMS_FILE" >&2
  echo "      create either (one term per line, # for comments) to also match" >&2
  echo "      client/employer names. Keep it OUT of this repo." >&2
  if [ -n "${GATEWAY_SENSITIVE_TERMS_REQUIRED:-}" ]; then
    echo "error: GATEWAY_SENSITIVE_TERMS_REQUIRED is set and no wordlist was found" >&2
    exit 2
  fi
fi

if [ "$status" -eq 0 ]; then
  # Never a bare "clean" when half the gate did not run. The caller reads this
  # line; the explanation on stderr is not where they look.
  if [ "$wordlist_ran" -eq 1 ]; then
    echo "check-sensitive-terms: clean"
  else
    echo "check-sensitive-terms: clean (STRUCTURAL ONLY -- no name wordlist, the client/employer/project name half did NOT run)"
  fi
fi
exit "$status"
