# Gateway Service

Gateway Service 是平台的统一 HTTP 入口，负责聚合各后端服务的 OpenAPI 文档、提供统一 Swagger UI，并把请求反向代理到 data、signal、analysis、trade 等服务。

## Scope

- 聚合多个后端服务的 OpenAPI spec
- 提供统一的 docs 页面
- 提供跨服务 health 检查
- 反向代理到各注册服务
- 支持通过环境变量覆盖各服务后端地址

核心文件：

- `services/gateway_service/app/main.py`
- `services/gateway_service/app/registry.py`
- `services/gateway_service/app/routes.py`
- `services/gateway_service/app/proxy.py`
- `services/gateway_service/app/docs.py`

## Architecture Overview

Gateway Service 自身不承载业务逻辑，只做两件事：

- 文档聚合
- 请求转发

启动时会：

- 初始化 logging
- 建立共享 `httpx.AsyncClient`
- 从默认配置和环境变量构造 service registry

运行时有三块核心模块：

- `registry`: 维护逻辑服务名到 URL 的映射
- `routes`: health 和 spec cache 管理
- `proxy`: `/{service}/{path}` 形式的反向代理

## Registered Services

当前默认注册：

- `data` → `http://algo_data_service:8001`
- `signal` → `http://algo_signal_service:8002`
- `analysis` → `http://algo_analysis_service:8003`
- `trade` → `http://algo_trade_service:8004`

可用环境变量覆盖：

- `GATEWAY_DATA_URL`
- `GATEWAY_SIGNAL_URL`
- `GATEWAY_ANALYSIS_URL`
- `GATEWAY_TRADE_URL`

注意：当前 registry 里没有 `monitoring` 服务注册项，所以设置 `GATEWAY_MONITORING_URL` 不会生效，除非先把它加入 registry。

## Proxy Model

Gateway 的代理路径格式是：

- `/{service}/{path}`

例如：

- `/signal/api/v1/signals`
- `/analysis/api/v1/analysis/blueprint/2026-04-17`
- `/trade/api/v1/trade/portfolio/snapshot`

如果请求的 `service` 不在 registry 中，gateway 返回 `404`。

如果上游服务超时，gateway 返回 `504`。

如果转发过程抛出其他异常，gateway 返回 `502`。

## OpenAPI And Docs

Gateway 会从各后端服务拉取 OpenAPI spec，并生成统一文档入口。

常用端点：

- `GET /docs`
- `GET /openapi.json`
- `GET /openapi/gateway.json`
- `GET /openapi/{service}.json`
- `GET /specs/refresh`

注意：当前 `refresh specs` 是 `GET /specs/refresh`，不是 `POST`。

## Health Endpoints

- `GET /api/v1/health`
- `GET /api/v1/health/all`
- `GET /health` → 307 redirect
- `GET /health/all` → 307 redirect

`/api/v1/health/all` 会主动探测所有已注册后端的 `/api/v1/health`。

## Common Usage Examples

通过 gateway 查询 signals：

```bash
curl "http://localhost:8000/signal/api/v1/signals?symbols=AAPL,MSFT"
```

通过 gateway 查询 analysis blueprint：

```bash
curl "http://localhost:8000/analysis/api/v1/analysis/blueprint/2026-04-17"
```

通过 gateway 查询 trade portfolio snapshot：

```bash
curl "http://localhost:8000/trade/api/v1/trade/portfolio/snapshot"
```

刷新聚合文档缓存：

```bash
curl http://localhost:8000/specs/refresh
```

## Local Run

从仓库根目录运行：

```bash
# 1) Install deps
uv sync --package gateway-service

# 2) Point gateway at local services
export GATEWAY_DATA_URL=http://127.0.0.1:8001
export GATEWAY_SIGNAL_URL=http://127.0.0.1:8002
export GATEWAY_ANALYSIS_URL=http://127.0.0.1:8003
export GATEWAY_TRADE_URL=http://127.0.0.1:8004

# 3) Start gateway
uv run uvicorn services.gateway_service.app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Dev Notes

- Gateway 不替代服务发现系统，它只是一个静态 registry + env override 模型
- Gateway 自己没有数据库依赖，但如果后端服务没启动，代理和 `/health/all` 会反映失败
- 文档或路由变更后，如果聚合 spec 没刷新，先调用 `/specs/refresh`
- README 中的代理示例都按当前代码真实路径编写，不对下游服务路径做“美化”
