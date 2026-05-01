#!/usr/bin/env bash
# ── Taiwan Real Estate Dashboard — GitHub Pages Deploy ─────────────────────
# Usage:
#   bash deploy.sh <github-username> [repo-name]
#
# Example:
#   bash deploy.sh johndoe
#   bash deploy.sh johndoe my-realestate-dashboard
#
# What this does:
#   1. Adds a GitHub remote (or updates it)
#   2. Pushes main branch
#   3. Enables GitHub Pages via gh CLI (if installed)
#   4. Prints the live URL

set -euo pipefail

GH_USER="${1:-}"
REPO_NAME="${2:-taiwan-realestate-radar}"

if [ -z "$GH_USER" ]; then
  echo "Usage: bash deploy.sh <github-username> [repo-name]"
  exit 1
fi

REMOTE_URL="https://github.com/${GH_USER}/${REPO_NAME}.git"
PAGES_URL="https://${GH_USER}.github.io/${REPO_NAME}/"

echo "── Setting remote to ${REMOTE_URL} …"
if git remote get-url origin &>/dev/null; then
  git remote set-url origin "${REMOTE_URL}"
else
  git remote add origin "${REMOTE_URL}"
fi

echo "── Pushing to GitHub …"
git push -u origin main

# Enable GitHub Pages via gh CLI if available
if command -v gh &>/dev/null; then
  echo "── Enabling GitHub Pages (root of main) …"
  gh api \
    --method PUT \
    -H "Accept: application/vnd.github+json" \
    "/repos/${GH_USER}/${REPO_NAME}/pages" \
    -f source='{"branch":"main","path":"/"}' \
    --silent || true
fi

echo ""
echo "✅ Done!"
echo ""
echo "   Live URL (active within ~60 seconds):"
echo "   ${PAGES_URL}"
echo ""
echo "   GitHub Actions auto-refresh: daily at 02:00 UTC (10:00 AM TST) via update_data.yml."
echo "   Manual trigger: Actions → Update Real Estate Data → Run workflow"
echo "   Manual backfill: Actions → Backfill Historical Data → Run workflow"
