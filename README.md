# From your repo root:
mkdir -p .github/workflows
cp daily-us-update.yml .github/workflows/daily-us-update.yml
git add .github/workflows/daily-us-update.yml
git commit -m "Add daily US market data cron (S&P 500 + NASDAQ)"
git push
