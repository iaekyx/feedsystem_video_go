# feedsystem_video_go

基于 Go + Vue 3 的短视频 Feed 系统，含账号、视频、点赞、评论、关注、Feed 流、私信、通知，支持 Redis 缓存、RabbitMQ 异步 Worker、分片上传、SSE 实时推送、Docker Compose 部署。

## 更完整的视频 Feed 流系统项目

[LeoninCS/GCFeed](https://github.com/LeoninCS/GCFeed) 是更为全面完整的视频 Feed 流系统项目，覆盖更丰富的业务能力与工程实践。

## 功能

| 模块 | 功能 |
|------|------|
| 账号 | 注册、登录、Refresh Token、改名、改密、登出、头像上传、个人简介、主页统计 |
| 视频 | 普通上传、5MB 分片上传、断点续传、封面上传、发布、作者作品、详情缓存、#话题标签 |
| 点赞 | 点赞、取消点赞、是否已赞、已赞列表、RabbitMQ 异步落库、热度更新、SSE 通知 |
| 评论 | 发布、删除、列表、@username 提及通知、RabbitMQ 异步落库、热度更新 |
| 关注 | 关注、取关、粉丝列表、关注列表、粉丝/关注计数、SSE 通知 |
| Feed | 推荐流、关注流、点赞榜、热榜、话题流、冷热分离、游标分页、短视频沉浸播放 |
| 私信 | 发送私信、按对端用户查看最近 50 条会话 |
| 通知 | 点赞/评论/关注事件通知、提及通知、SSE 实时推送、通知列表、未读计数、已读标记 |
| 工程 | Docker Compose、`start.sh`、API/Worker 拆分运行、限流、pprof、健康检查 |

## Docker Compose 一键启动

```bash
docker compose up -d --build
```

访问：
- 前端：`http://localhost:5173`
- 后端 API：`http://localhost:8080`
- RabbitMQ 管理台：`http://localhost:15672`（`admin` / `password123`）

Docker Compose 会读取 `.env`，缺省使用 `feedsystem-dev-secret-key`。生产环境请修改 `JWT_SECRET`。

## 脚本启动

```bash
./start.sh
```

`start.sh` 默认启动 RabbitMQ、Redis、后端 API、Worker 与前端。常用开关：

```bash
START_FRONTEND=0 ./start.sh       # API + Worker
START_WORKER=0 ./start.sh         # API + 前端
STOP_DOCKER=1 ./start.sh          # 退出时停止脚本拉起的 compose 服务
CONFIG_PATH=configs/config.yaml ./start.sh
```

## 本地开发

```bash
# 启动依赖
docker compose up -d mysql redis rabbitmq

# 后端
cd backend
CONFIG_PATH=configs/config.compose-local.yaml go run ./cmd

# Worker
CONFIG_PATH=configs/config.compose-local.yaml go run ./cmd/worker

# 前端
cd frontend
npm install && npm run dev
```

## CI

GitHub Actions 配置位于 `.github/workflows/ci.yml`，在 Pull Request 以及推送到 `main`、`master` 时运行。

- 后端：Go 1.24.x，执行 `go mod download`、`go vet ./...`、`go test -race -count=1 ./...`
- 前端：Node.js 22，执行 `npm ci`、`npm run build`

## 接口清单

### 账号 `/account`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/register` | 否 | 注册（限流 5次/时/IP） |
| POST | `/login` | 否 | 登录，返回 access_token + refresh_token |
| POST | `/refresh` | 否 | 刷新 access_token（用 refresh_token） |
| POST | `/changePassword` | 否 | 改密码（需旧密码） |
| POST | `/findByID` | 否 | 按 ID 查用户 |
| POST | `/findByUsername` | 否 | 按用户名查 |
| POST | `/getProfile` | 否 | 用户主页（视频数/获赞/粉丝数） |
| POST | `/logout` | JWT | 登出（同时失效双 token） |
| POST | `/rename` | JWT | 改名 |
| POST | `/uploadAvatar` | JWT | 上传头像（jpg/png/webp，≤10MB） |
| POST | `/updateProfile` | JWT | 更新简介/头像 |

### 视频 `/video`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/publish` | JWT | 发布视频（自动提取 #话题） |
| POST | `/uploadVideo` | JWT | 上传视频文件（mp4，≤200MB） |
| POST | `/uploadCover` | JWT | 上传封面（jpg/png/webp，≤10MB） |
| POST | `/chunk/init` | JWT | 初始化分片上传（文件 MD5、大小、分片数） |
| POST | `/chunk/upload` | JWT | 上传单个分片（multipart，含分片 MD5 校验） |
| POST | `/chunk/status` | JWT | 查询已上传分片 |
| POST | `/chunk/complete` | JWT | 合并分片并返回 play_url |
| POST | `/listByAuthorID` | 否 | 按作者查视频 |
| POST | `/getDetail` | 否 | 视频详情缓存 |

### 点赞 `/like`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/like` | JWT | 点赞 |
| POST | `/unlike` | JWT | 取消点赞 |
| POST | `/isLiked` | JWT | 是否已赞 |
| POST | `/listMyLikedVideos` | JWT | 我赞过的视频 |

### 评论 `/comment`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/listAll` | 否 | 评论列表（分页200，按时间升序） |
| POST | `/publish` | JWT | 发布评论（支持 @username 提及） |
| POST | `/delete` | JWT | 删除评论 |

### 关注 `/social`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/follow` | JWT | 关注 |
| POST | `/unfollow` | JWT | 取关 |
| POST | `/getAllFollowers` | JWT | 粉丝列表（含粉丝数） |
| POST | `/getAllVloggers` | JWT | 关注列表（含关注数） |
| POST | `/getCounts` | JWT | 粉丝/关注计数 |

### Feed `/feed`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/listLatest` | 软鉴权 | 最新视频（游标分页） |
| POST | `/listLikesCount` | 软鉴权 | 点赞排行（复合游标） |
| POST | `/listByPopularity` | 软鉴权 | 热度榜（快照分页） |
| POST | `/listByFollowing` | JWT | 关注流 |
| POST | `/listByTag` | 软鉴权 | 按 #话题 浏览 |

### 通知 `/notification`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| GET | `/stream?token=<access_token>` | 是 | SSE 实时推送，也支持 `Authorization: Bearer <token>` |
| POST | `/list` | 是 | 最近 50 条通知 |
| POST | `/markRead` | 是 | 标记已读；传 `id` 标记单条，省略 `id` 标记全部 |
| POST | `/unreadCount` | 是 | 未读计数 |

### 私信 `/message`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/send` | JWT | 发送私信 |
| POST | `/list` | JWT | 对话列表 |

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `JWT_SECRET` | `feedsystem-dev-secret-key` | JWT 签名密钥，生产须改 |
| `SERVER_PORT` | `8080` | 后端监听端口 |
| `MYSQL_HOST` / `MYSQL_PORT` | 配置文件值 | MySQL 地址 |
| `MYSQL_USER` / `MYSQL_PASSWORD` | 配置文件值 | MySQL 账号密码 |
| `MYSQL_ROOT_PASSWORD` | `123456` | MySQL root 密码 |
| `MYSQL_DATABASE` | `feedsystem` | MySQL 数据库名 |
| `REDIS_HOST` / `REDIS_PORT` | 配置文件值 | Redis 地址 |
| `REDIS_PASSWORD` | `123456` | Redis 密码 |
| `REDIS_DB` | `0` | Redis DB |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | 配置文件值 | RabbitMQ 地址 |
| `RABBITMQ_USER` / `RABBITMQ_PASS` | `admin` / `password123` | RabbitMQ 账号 |

详见 `.env.example`。

## 运维与可观测性

### API / Worker 职责与停机

- API：HTTP、鉴权与同步业务；管理 SSE 连接，通过每实例独立的 RabbitMQ 临时队列接收通知广播。`router.go` 只组装路由，不启动后台消费者。
- Worker：点赞、评论、关注、热度消费者，以及 Outbox 轮询、全局时间线更新、通知生成与持久化。
- 点赞和取消点赞消费在一个 MySQL 事务里修改关系记录、点赞数和热度；对视频行加锁，重复的同方向操作不会再次增加/扣减计数。这不是跨点赞/取消点赞乱序事件的完整去重协议。
- Outbox 使用 `FOR UPDATE SKIP LOCKED` 在事务内领取单条记录，等待 Publisher Confirm 并检查不可路由返回后删除记录；发布失败回滚，后续重试。事务期间会持有行锁，单次确认等待上限为 3 秒。确认成功后事务失败仍可能重复投递，时间线 `ZADD` 可重复执行。
- 通知按源消息内容指纹去重落库，然后广播。所有在线 API 实例各接收一份，只推给自己持有的 SSE 连接。实时推送是尽力而为；断线期间或队列溢出的消息通过通知列表接口恢复，不能依赖 SSE 保存历史。API 对一分钟内重复通知 ID 做本地过滤。
- `SIGINT`/`SIGTERM` 会取消后台任务并等待退出，再关闭依赖连接。Worker 停止时取消在途操作，未确认 MQ 消息可重新投递。API 同时结束 SSE 长连接，并给普通 HTTP 请求最多 5 秒完成时间。
- Consumer 重试耗尽后 Nack 到配置的死信交换机，需要人工检查/重放。原有点赞与 Redis 热度的双发布仍不是原子操作；本次拆分不解决跨 MySQL/Redis 的全部一致性问题。
- API 负责自动迁移表结构；Compose 首次启动等待 API 健康后再启动 Worker。通知新增可空唯一字段 `event_key`，已有通知记录可保留。正式多实例部署应把迁移作为单独步骤。

API、Worker 默认各限制为 1 CPU、512 MiB，可通过 `API_CPUS`、`API_MEMORY`、`WORKER_CPUS`、`WORKER_MEMORY` 覆盖。Worker 可使用 `docker compose up -d --scale worker=2` 扩容。API 扩容还需调整固定宿主端口映射并配置负载均衡。

隔离集成测试（启动独立 MySQL / RabbitMQ，不连接业务数据库）：

```bash
docker volume create feedsystem_go_mod
docker volume create feedsystem_go_build
docker compose -p feedsystem-integration -f compose.integration.yml up --abort-on-container-exit --exit-code-from tests
docker compose -p feedsystem-integration -f compose.integration.yml down -v
```

测试覆盖事务失败回滚、重复点赞/取消、Outbox 并发领取与失败保留、通知重试去重、跨 API 队列广播、不可路由发布确认，以及任务退出；执行全量 `go test -race` 和 `go vet`。临时数据库使用 tmpfs，Go 缓存卷可复用。

- `GET /healthz` 返回后端健康状态。
- 本地配置默认开启 pprof：API `localhost:6060`，Worker `localhost:6061`。
- 上传文件写入 `backend/.run/uploads`；Docker 环境挂载到 `backend_uploads` volume。
- Redis 用于 Token 缓存、视频实体缓存、Feed 时间线、热榜窗口、分片上传会话。
- RabbitMQ Topic Exchange 覆盖点赞、评论、关注、热度、视频时间线事件，并配置 DLX。
