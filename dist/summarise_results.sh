#!/bin/bash

{
  echo "$TITLE"
  echo "Found $(jq -r 'length' "$ANNOTATIONS_PATH") issues"
  echo ""
} >> "$GITHUB_STEP_SUMMARY"

jq -r "[.[] | .raw_details.result_type] | unique | .[]" "$ANNOTATIONS_PATH" | while read -r group_name; do
  echo "## $group_name"
  type="$(echo "$group_name" | sed -r "s|s$||")"

  if [[ $(jq -r '[.[] | .raw_details.parent_name] | unique | length == 1 and .[0] == null' "$ANNOTATIONS_PATH") ]]; then
    echo "| File | $type | Enforcement | Message |"
    echo "| ---- | ----- | ----------- | ------- |"
    name_format='\(.raw_details.name)'
    sort_by="sort_by(.raw_details.name)"
  else
    parent_type="$(jq -r '[.[] | .raw_details.parent_type][0]' "$ANNOTATIONS_PATH" | sed -r "s|s$||")"
    echo "| File | $parent_type | $type | Enforcement | Message |"
    echo "| ---- | ------------ | ----- | ----------- | ------- |"
    name_format='\(.raw_details.parent_name) | \(.raw_details.name)'
    sort_by="sort_by(.raw_details.parent_name) | sort_by(.raw_details.name)"
  fi

  sort_by="sort_by(.path) | $sort_by | sort_by(.start_line) | sort_by(.title)"

  file_url_line_format="\(if .start_line == null or .end_line == null then \"\" else \"#L\(.start_line)-L\(.end_line - 1)\" end)"
  file_format="[\(.path)]($GITHUB_SERVER_URL/$GITHUB_REPOSITORY/blob/$GITHUB_SHA/$FILE_DIR/\(.path)$file_url_line_format)"
  enforcement_format='\(.title)'
  message_format='\(.message)'

  filter_format="select(.raw_details.result_type == \"$group_name\")"
  line_format="| $file_format | $name_format | $enforcement_format | $message_format |"
  jq -rs ".[] | $sort_by | .[] | $filter_format | \"$line_format\"" "$ANNOTATIONS_PATH"

  echo ""
done >> "$GITHUB_STEP_SUMMARY"