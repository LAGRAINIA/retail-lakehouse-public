#!/usr/bin/env bash
# (Re)create an Astro Deployment for one environment and wire it to the CI/CD.
#
#   scripts/create_deployment.sh prod        # or uat
#
# Steps: create the Deployment -> load its variables and secrets from .env -> create a Deployment API
# token for GitHub Actions -> store token and Deployment id in the GitHub Environment -> deploy the
# code of the matching branch (uat or main) from a clean checkout. Secrets never appear on a command line.
# Requires: astro login, gh auth login, a filled .env.
set -euo pipefail

ENV_NAME="${1:?usage: $0 <uat|prod>}"
case "$ENV_NAME" in
  uat)  DEV_MODE=enable;  BRANCH=uat ;;
  prod) DEV_MODE=disable; BRANCH=main ;;
  *) echo "environment must be uat or prod" >&2; exit 1 ;;
esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO="${GITHUB_REPO:-LAGRAINIA/retail-lakehouse}"
WORKSPACE_ID="${ASTRO_WORKSPACE_ID:-$(astro workspace list 2>/dev/null | awk 'NR==2 {print $2}')}"
RUNTIME="$(sed -n 's#^FROM .*/runtime:\(.*\)$#\1#p' "$ROOT/Dockerfile")"
NAME="retail-$ENV_NAME"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

echo "1/5 create Deployment $NAME (runtime $RUNTIME)"
astro deployment create --name "$NAME" --description "Retail lakehouse - $ENV_NAME" \
  --cloud-provider aws --region us-east-1 --runtime-version "$RUNTIME" --executor CeleryExecutor \
  --scheduler-size small --development-mode "$DEV_MODE" --workspace-id "$WORKSPACE_ID" --force --wait >/dev/null
DEPLOYMENT_ID="$(astro deployment list --workspace-id "$WORKSPACE_ID" 2>/dev/null | awk -v n="$NAME" '$1==n {print $6}')"
echo "    id: $DEPLOYMENT_ID"

echo "2/5 load variables and secrets from .env"
python3 - "$ROOT/.env" "$ENV_NAME" "$TMP" <<'PY'
import sys
from pathlib import Path
env_file, env, out = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
v = dict(l.split("=", 1) for l in env_file.read_text().splitlines() if "=" in l and not l.lstrip().startswith("#"))
plain = {"DATABRICKS_HOST": v["DATABRICKS_HOST"], "DATABRICKS_HTTP_PATH": v["DATABRICKS_HTTP_PATH"],
         "DATABRICKS_CATALOG": f"retail_{env}", "DBT_TARGET": env,
         "RETAIL_EPOCH": v.get("RETAIL_EPOCH", "2026-07-01"), "RETAIL_SOURCES_SCHEDULE": "0 1 * * *"}
secret = {"DATABRICKS_TOKEN": v["DATABRICKS_TOKEN"],
          "AIRFLOW_CONN_DATABRICKS_DEFAULT": v["AIRFLOW_CONN_DATABRICKS_DEFAULT"].strip("'")}
(out / "plain.env").write_text("".join(f"{k}={x}\n" for k, x in plain.items()))
(out / "secret.env").write_text("".join(f"{k}={x}\n" for k, x in secret.items()))
PY
astro deployment variable create -d "$DEPLOYMENT_ID" --load --env "$TMP/plain.env" >/dev/null
astro deployment variable create -d "$DEPLOYMENT_ID" --load --env "$TMP/secret.env" --secret >/dev/null

echo "3/5 create the GitHub Actions token"
TOKEN="$(astro deployment token create --deployment-id "$DEPLOYMENT_ID" --name github-actions \
  --description "CI/CD deploys from GitHub Actions" --role DEPLOYMENT_ADMIN --expiration 90 --clean-output 2>&1)"
if [ "${#TOKEN}" -lt 200 ] || printf '%s' "$TOKEN" | grep -qi error; then
  echo "token creation failed" >&2; exit 1
fi

echo "4/5 update the GitHub Environment '$ENV_NAME'"
printf '%s' "$TOKEN" | gh secret set ASTRO_API_TOKEN --env "$ENV_NAME" -R "$REPO"
unset TOKEN
gh variable set ASTRO_DEPLOYMENT_ID --env "$ENV_NAME" --body "$DEPLOYMENT_ID" -R "$REPO"

echo "5/5 deploy the code of branch $BRANCH (needs Docker running)"
git -C "$ROOT" fetch -q origin "$BRANCH"
git -C "$ROOT" worktree add -q --detach "$TMP/src" "origin/$BRANCH"
(cd "$TMP/src" && astro deploy "$DEPLOYMENT_ID" --force)
git -C "$ROOT" worktree remove --force "$TMP/src"
echo "Done. Follow the deploy in GitHub Actions, then load the history:"
echo "  Airflow > retail_source_systems > Trigger with start_date=$(grep '^RETAIL_EPOCH=' "$ROOT/.env" | cut -d= -f2) and end_date=<yesterday>"
