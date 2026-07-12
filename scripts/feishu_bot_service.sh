#!/bin/bash
# =====================================================================
# 飞书机器人 launchd 常驻服务管理脚本（macOS）
#
# 用法：
#   ./scripts/feishu_bot_service.sh install    # 安装：开机自启 + 崩溃自动拉起，装完立即启动
#   ./scripts/feishu_bot_service.sh uninstall  # 卸载：停止并移除服务
#   ./scripts/feishu_bot_service.sh status     # 查看运行状态
#   ./scripts/feishu_bot_service.sh restart    # 重启（改完 local.yaml / 更新代码后用）
#   ./scripts/feishu_bot_service.sh log        # 实时看日志
#
# 说明：
# - 服务装在当前登录用户下（gui 域），登录后自动启动
# - 进程异常退出 30 秒后自动拉起；手动 uninstall 才会真正停掉
# - 找不到虚拟环境时，可指定：VENV_PYTHON=/path/to/python ./scripts/feishu_bot_service.sh install
# =====================================================================

set -euo pipefail

LABEL="com.stockagent.feishubot"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"
OUT_LOG="$PROJECT_DIR/logs/feishu_bot.out.log"
ERR_LOG="$PROJECT_DIR/logs/feishu_bot.err.log"
GUI_DOMAIN="gui/$(id -u)"

find_python() {
    # 1. 显式指定优先
    if [ -n "${VENV_PYTHON:-}" ]; then
        echo "$VENV_PYTHON"
        return
    fi
    # 2. 常见虚拟环境位置
    local candidates=(
        "$PROJECT_DIR/python_stock_env/bin/python"
        "$PROJECT_DIR/../python_stock_env/bin/python"
        "$HOME/python_stock_env/bin/python"
        "$PROJECT_DIR/venv/bin/python"
        "$PROJECT_DIR/.venv/bin/python"
    )
    for p in "${candidates[@]}"; do
        if [ -x "$p" ]; then
            echo "$p"
            return
        fi
    done
    echo ""
}

do_install() {
    local py
    py="$(find_python)"
    if [ -z "$py" ]; then
        echo "❌ 没找到虚拟环境 python。请指定后重试："
        echo "   VENV_PYTHON=/你的venv路径/bin/python $0 install"
        exit 1
    fi
    echo "使用 Python: $py"

    # 依赖自检：缺关键包就提前报，别等 launchd 起来无限崩溃重启
    if ! "$py" -c "import lark_oapi, schedule, langgraph" 2>/dev/null; then
        echo "❌ 该环境缺少依赖（lark_oapi/schedule/langgraph 至少一个装不上）。先执行："
        echo "   $py -m pip install -r $PROJECT_DIR/requirements.txt"
        exit 1
    fi

    mkdir -p "$PROJECT_DIR/logs" "$HOME/Library/LaunchAgents"

    # 已装过则先卸旧的（幂等，可反复执行升级）
    launchctl bootout "$GUI_DOMAIN" "$PLIST" 2>/dev/null || true

    cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${py}</string>
        <string>${PROJECT_DIR}/feishu_bot.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${PROJECT_DIR}</string>
    <!-- 登录后自动启动 -->
    <key>RunAtLoad</key>
    <true/>
    <!-- 异常退出自动拉起（正常退出码 0 不拉，方便手动停） -->
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <!-- 崩溃后至少间隔 30 秒再拉，防止配置错了无限快速重启 -->
    <key>ThrottleInterval</key>
    <integer>30</integer>
    <key>StandardOutPath</key>
    <string>${OUT_LOG}</string>
    <key>StandardErrorPath</key>
    <string>${ERR_LOG}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin</string>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
    </dict>
</dict>
</plist>
EOF

    launchctl bootstrap "$GUI_DOMAIN" "$PLIST"
    launchctl enable "$GUI_DOMAIN/$LABEL"
    sleep 2
    do_status
    echo ""
    echo "✅ 安装完成：登录自启 + 崩溃自动拉起 已生效"
    echo "   日志: $0 log    重启: $0 restart    卸载: $0 uninstall"
}

do_uninstall() {
    launchctl bootout "$GUI_DOMAIN" "$PLIST" 2>/dev/null || true
    rm -f "$PLIST"
    echo "✅ 已停止并卸载服务（日志文件保留在 logs/ 下）"
}

do_status() {
    if launchctl print "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1; then
        local pid
        pid=$(launchctl print "$GUI_DOMAIN/$LABEL" 2>/dev/null | awk '/^[[:space:]]*pid = /{print $3}')
        if [ -n "${pid:-}" ]; then
            echo "🟢 运行中 (pid=$pid)"
        else
            echo "🟡 已注册但进程未在跑（可能正处于崩溃重启间隔，看日志: $0 log）"
        fi
    else
        echo "⚪ 未安装（执行 $0 install）"
    fi
}

do_restart() {
    launchctl kickstart -k "$GUI_DOMAIN/$LABEL"
    sleep 2
    do_status
}

do_log() {
    echo "===== 实时日志（Ctrl+C 退出） ====="
    touch "$OUT_LOG" "$ERR_LOG"
    tail -f "$OUT_LOG" "$ERR_LOG"
}

case "${1:-}" in
    install)   do_install ;;
    uninstall) do_uninstall ;;
    status)    do_status ;;
    restart)   do_restart ;;
    log)       do_log ;;
    *)
        echo "用法: $0 {install|uninstall|status|restart|log}"
        exit 1
        ;;
esac
