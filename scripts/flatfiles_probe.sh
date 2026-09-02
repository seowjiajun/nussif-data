#!/usr/bin/env bash
# Probe Massive flat-file S3 access WITHOUT touching ~/.aws default profile.
# LIST calls are free / unmetered; this downloads exactly one small day file.
#
# Prereq: /home/jseow/code/.secrets/massive_s3.env with
#   AWS_ACCESS_KEY_ID=...
#   AWS_SECRET_ACCESS_KEY=...
set -euo pipefail

SECRETS="/home/jseow/code/.secrets/massive_s3.env"
ENDPOINT="https://files.massive.com"
BUCKET="flatfiles"
SCRATCH="/tmp/claude-1000/-home-jseow-code/2c476698-0bd0-41bc-8bd0-7d4063f254ac/scratchpad"

[ -f "$SECRETS" ] || { echo "missing $SECRETS"; exit 1; }

# isolate: only these creds, ignore ambient AWS_PROFILE / instance metadata
set -a; . "$SECRETS"; set +a
unset AWS_PROFILE AWS_DEFAULT_PROFILE || true
export AWS_EC2_METADATA_DISABLED=true
export AWS_REGION="${AWS_REGION:-us-east-1}"

S3=(aws s3 --endpoint-url "$ENDPOINT")
S3API=(aws s3api --endpoint-url "$ENDPOINT")

echo "### 1. bucket root"
"${S3[@]}" ls "s3://$BUCKET/" || { echo "AUTH/ACCESS FAILED"; exit 2; }

echo; echo "### 2. options prefixes"
"${S3[@]}" ls "s3://$BUCKET/us_options_opra/" || true
"${S3[@]}" ls "s3://$BUCKET/options/" 2>/dev/null || true

echo; echo "### 3. day-aggregates: earliest + latest year/month"
for P in us_options_opra/day_aggs_v1 us_options_opra/day_aggregates_v1 options/day-aggregates; do
  if "${S3[@]}" ls "s3://$BUCKET/$P/" >/dev/null 2>&1; then
    echo "-- $P"
    "${S3[@]}" ls "s3://$BUCKET/$P/"
    FIRST_Y=$("${S3[@]}" ls "s3://$BUCKET/$P/" | awk '{print $2}' | head -1)
    LAST_Y=$("${S3[@]}"  ls "s3://$BUCKET/$P/" | awk '{print $2}' | tail -1)
    echo "-- earliest year $FIRST_Y :"; "${S3[@]}" ls "s3://$BUCKET/$P/${FIRST_Y}" | head
    echo "-- latest year   $LAST_Y :"; "${S3[@]}" ls "s3://$BUCKET/$P/${LAST_Y}" | tail
    DAYP="$P"
  fi
done

echo; echo "### 4. one sample options day-aggs file (schema + size)"
if [ -n "${DAYP:-}" ]; then
  # pick a known good trading day
  for D in 2024/06/2024-06-03 2024/06/03 2023/06/2023-06-01; do
    KEY="$DAYP/$D.csv.gz"
    if "${S3API[@]}" head-object --bucket "$BUCKET" --key "$KEY" >/dev/null 2>&1; then
      SZ=$("${S3API[@]}" head-object --bucket "$BUCKET" --key "$KEY" --query ContentLength --output text)
      echo "found $KEY  ($(numfmt --to=iec "$SZ"))"
      "${S3[@]}" cp "s3://$BUCKET/$KEY" "$SCRATCH/sample_opt_day.csv.gz" --no-progress
      echo "--- header + first rows ---"
      zcat "$SCRATCH/sample_opt_day.csv.gz" | head -5
      echo "--- total rows ---"
      zcat "$SCRATCH/sample_opt_day.csv.gz" | wc -l
      echo "--- rows for our 5 underliers (O:SPY/QQQ/IWM/TLT/GLD) ---"
      zcat "$SCRATCH/sample_opt_day.csv.gz" | grep -cE '(^|,)O:(SPY|QQQ|IWM|TLT|GLD)[0-9]' || true
      break
    fi
  done
fi

echo; echo "### 5. stocks day-aggs prefix (for underlying bars, same bucket)"
for P in us_stocks_sip/day_aggs_v1 stocks/day-aggregates; do
  "${S3[@]}" ls "s3://$BUCKET/$P/" 2>/dev/null | head && break || true
done

echo; echo "### 6. options quotes prefix present? (EOD NBBO source; big)"
for P in us_options_opra/quotes_v1 options/quotes; do
  "${S3[@]}" ls "s3://$BUCKET/$P/" 2>/dev/null | head && { echo "quotes AVAILABLE at $P"; break; } || true
done

echo; echo "### done"
