#!/usr/bin/env bash
# 公开演示:用 Cloudflare Quick Tunnel(无需账号和域名)把本机 8080 暴露成 https://<随机>.trycloudflare.com。
# 前置:brew install cloudflared;.env 里配置好 DEMO_ACCESS_CODE 和 DEMO_TRUST_PROXY_HEADER=true。
# 隧道从本机出站连 Cloudflare,不用开端口;Ctrl-C 即下线。Quick Tunnel 每次启动 URL 都会变,
# 要固定域名改用 named tunnel(cloudflared tunnel create / route dns,需要 Cloudflare 账号里的域名)。
set -euo pipefail
cd "$(dirname "$0")/.."

env_val() { grep -E "^$1=" .env 2>/dev/null | tail -1 | cut -d= -f2- || true; }

[ -n "$(env_val DEMO_ACCESS_CODE)" ] || { echo "拒绝启动:先在 .env 里设置 DEMO_ACCESS_CODE(公开演示必须有访问码)"; exit 1; }
[ "$(env_val DEMO_TRUST_PROXY_HEADER)" = "true" ] || {
  echo "拒绝启动:经隧道访问时所有请求的对端都是本机,需要 DEMO_TRUST_PROXY_HEADER=true 才能按真实 IP 限流"; exit 1; }
case "$(env_val SERVER_BIND)" in
  ""|127.0.0.1) ;;
  *) echo "拒绝启动:SERVER_BIND 不是 127.0.0.1 时,别人可以绕过隧道直连 8080 并伪造 CF-Connecting-IP"; exit 1;;
esac
command -v cloudflared >/dev/null || { echo "缺少 cloudflared:brew install cloudflared"; exit 1; }

docker compose up -d
echo "每 IP 每天 $(env_val DEMO_PER_IP_DAILY | sed 's/^$/5/') 次 · 每日预算 \$$(env_val DEMO_DAILY_BUDGET_USD | sed 's/^$/1.0/')"
exec cloudflared tunnel --no-autoupdate --url http://localhost:8080
