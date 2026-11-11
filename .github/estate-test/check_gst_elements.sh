#!/usr/bin/env bash
# Report each named GStreamer element into the job summary and fail when one
# is missing (TIN-8227). The audio and pipeline tests that follow this job
# need these elements, so a runner image without them fails here with the
# element's name, not later inside a test.
#
# Usage: check_gst_elements.sh ELEMENT...
# GST_INSPECT overrides the inspector, default gst-inspect-1.0.
set -euo pipefail

inspect="${GST_INSPECT:-gst-inspect-1.0}"
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"

if [ "$#" -eq 0 ]; then
  echo "check_gst_elements: no element named" >&2
  exit 2
fi

missing=()
{
  echo "### GStreamer elements"
  echo
  echo "| element | plugin | version |"
  echo "|---|---|---|"
} >>"$summary"

for element in "$@"; do
  if out="$("$inspect" "$element" 2>/dev/null)"; then
    # gst-inspect-1.0 prints the plugin's Name and Version under "Plugin Details:".
    plugin="$(printf '%s\n' "$out" | awk '/^Plugin Details:/{p=1;next} p&&$1=="Name"{print $2;exit}')"
    version="$(printf '%s\n' "$out" | awk '/^Plugin Details:/{p=1;next} p&&$1=="Version"{print $2;exit}')"
    echo "| \`$element\` | ${plugin:-?} | ${version:-?} |" >>"$summary"
    echo "found $element ${version:-}"
  else
    echo "| \`$element\` | missing | |" >>"$summary"
    echo "missing $element" >&2
    missing+=("$element")
  fi
done

if [ "${#missing[@]}" -gt 0 ]; then
  echo "check_gst_elements: missing ${missing[*]}" >&2
  exit 1
fi
