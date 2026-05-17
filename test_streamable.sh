#!/bin/bash
set -e

MCP_URL="http://localhost:8080/mcp"

echo "=== 测试 1: 健康检查 ==="
curl -s http://localhost:8080/health | python3 -m json.tool
echo ""

echo "=== 测试 2: Initialize 握手 ==="
RESPONSE=$(curl -s -D /tmp/mcp_headers.txt -X POST "$MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2025-03-26",
      "capabilities": {},
      "clientInfo": {"name": "test-client", "version": "1.0.0"}
    }
  }')
echo "$RESPONSE" | python3 -m json.tool

# 【关键修复】：加上 ^ 限制从行首匹配，并带上冒号
SESSION_ID=$(grep -i "^MCP-Session-Id:" /tmp/mcp_headers.txt | awk '{print $2}' | tr -d '\r')
echo "提取到的 Session ID: [$SESSION_ID]"
echo ""

echo "=== 测试 3: initialized 通知 ==="
curl -s -D - -X POST "$MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -H "MCP-Session-Id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}'
echo ""

echo "=== 测试 4: tools/list ==="
curl -s -X POST "$MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -H "MCP-Session-Id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' | python3 -m json.tool
echo ""

echo "=== 测试 5: tools/call (天气查询 - 北京) ==="
curl -s -X POST "$MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -H "MCP-Session-Id: $SESSION_ID" \
  -d '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "get_weather",
      "arguments": {"latitude":"39.9042","longitude":"116.4074","city":"Beijing"}
    }
  }' | python3 -m json.tool
echo ""

echo "=== 测试 6: ping ==="
curl -s -X POST "$MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -H "MCP-Session-Id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","id":7,"method":"ping"}' | python3 -m json.tool
echo ""

echo "=== 测试 7: 销毁会话 ==="
curl -s -D - -X DELETE "$MCP_URL" \
  -H "MCP-Session-Id: $SESSION_ID"
echo ""

echo "=== 所有测试完成 ==="