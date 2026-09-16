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
| `FEED_CELEBRITY_THRESHOLD` | `100000` | 粉丝数达到阈值使用拉模型；API 和 Worker 必须一致，须为正整数 |

详见 `.env.example`。

## 运维与可观测性

### API / Worker 职责与停机

- API：HTTP、鉴权与同步业务；管理 SSE 连接，通过每实例独立的 RabbitMQ 临时队列接收通知广播。`router.go` 只组装路由，不启动后台消费者。
- Worker：点赞、评论、关注、热度消费者，以及 Outbox 轮询、全局时间线更新、关注流扩散、通知生成与持久化。
- 点赞、取消点赞和评论请求先写入持久化 Outbox。消费者在一个 MySQL 事务中记录事件去重、修改业务数据和计数，并仅为真实变化写入热度 Outbox；旧消息重放不会恢复已取消的点赞或已删除的评论。
- Outbox 使用 `FOR UPDATE SKIP LOCKED` 在事务内领取单条记录，等待 Publisher Confirm 并检查不可路由返回后删除记录；发布失败回滚，后续重试。事务期间会持有行锁，单次确认等待上限为 3 秒。确认成功后事务失败仍可能重复投递，时间线 `ZADD` 可重复执行。
- 通知按源消息内容指纹去重落库，然后广播。所有在线 API 实例各接收一份，只推给自己持有的 SSE 连接。实时推送是尽力而为；断线期间或队列溢出的消息通过通知列表接口恢复，不能依赖 SSE 保存历史。API 对一分钟内重复通知 ID 做本地过滤。
- `SIGINT`/`SIGTERM` 会取消后台任务并等待退出，再关闭依赖连接。Worker 停止时取消在途操作，未确认 MQ 消息可重新投递。API 同时结束 SSE 长连接，并给普通 HTTP 请求最多 5 秒完成时间。
- Consumer 重试耗尽后 Nack 到配置的死信交换机，需要人工检查/重放。互动热度通过事务 Outbox 在业务提交后更新 Redis；发布等待 Broker 确认并检查不可路由返回，结果不确定时保留原事件重试。
- API 负责自动迁移表结构；Compose 首次启动等待 API 健康后再启动 Worker。通知新增可空唯一字段 `event_key`，已有通知记录可保留。正式多实例部署应把迁移作为单独步骤。

API、Worker 默认各限制为 1 CPU、512 MiB，可通过 `API_CPUS`、`API_MEMORY`、`WORKER_CPUS`、`WORKER_MEMORY` 覆盖。Worker 可使用 `docker compose up -d --scale worker=2` 扩容。API 扩容还需调整固定宿主端口映射并配置负载均衡。

隔离集成测试（启动独立 MySQL / Redis / RabbitMQ，不连接业务数据库）：

```bash
docker volume create feedsystem_go_mod
docker volume create feedsystem_go_build
docker compose -p feedsystem-integration -f compose.integration.yml up --abort-on-container-exit --exit-code-from tests
docker compose -p feedsystem-integration -f compose.integration.yml down -v
```

测试覆盖事务失败回滚、重复点赞/取消、Outbox 并发领取与失败保留、通知重试去重、跨 API 队列广播、不可路由发布确认，以及任务退出；执行全量 `go test -race` 和 `go vet`。临时数据库使用 tmpfs，Go 缓存卷可复用。

### 关注 Feed：推拉结合

实现位于 `backend/internal/followfeed`，由 `FollowingFanoutWorker` 更新缓存索引，API 调用 `Read` 后复用 `buildFeedVideos` 批量补充当前用户的 `is_liked`。不再缓存包含个人点赞状态的整页响应。

- Outbox 发布前声明时间线和关注流两条持久队列。一条已确认的 `video.timeline.publish` 事件分别路由到 `video.timeline.update.queue` 和 `video.following.fanout.queue`，不是两次独立发送。上线前遗留视频通过读时重建覆盖，无须重放旧 Outbox。
- 普通作者：更新 `feed:user_videos:<author>`，按粉丝 ID 每批 256 人分页，通过 Redis pipeline 将视频索引写入 `feed:inbox:<viewer>`。每个 inbox 的更新原子，但整批不跨 inbox 事务；部分失败可以安全重试。大 V：只更新作者索引，不逐粉丝写入。分类依据数据库当前粉丝数，默认阈值 100000，可用环境变量配置；本版没有持久化模式或双阈值滞回。
- 每次 Redis 写入用 Lua 原子完成 ZADD、保留最新 1000 条和设置 24 小时 TTL。ZSET 的 score 全为 0，member 为定长 `微秒时间戳:视频ID`，使用字典序范围查询，避免时间相同漏项和浮点打包精度损失。实际 key 还包含默认 `v1:` 前缀。
- 读时将普通作者 inbox 与关注的大 V 作品流做最大堆 k-way merge，按 `(create_time DESC, id DESC)` 排序并去重。读取 `limit+1` 条判断 `has_more`；拉取来源超过 32 个时整页退回 SQL，限制读放大。
- 缓存候选项按来源批量查库校验作者关系和删除状态，同时取得详情，避免旧 inbox 或过期实体缓存泄露取关/已删除内容。因此本版缓存命中仍会查库，不是纯 Redis 读链路。MySQL 增加 `(author_id, create_time DESC, id DESC)` 联合索引，由现有 AutoMigrate 创建。
- Redis 出错、来源缺失、历史页超出缓存范围或过滤后不足时，使用相同复合游标查询该来源的数据库数据。未登录用户不返回关注内容。
- 新关注、作者类别变化导致普通作者集合变化时，来源签名触发重建；合并写入而非整体覆盖，避免重建覆盖并发推送。旧作者残留项在读取时过滤。每个来源的就绪标记最长 60 秒后失效，由下一次读取重新查库修复，可补偿延迟事件和模式往返切换；只收到一次推送不会将不完整索引标记为就绪。
- 消费成功后 Ack，失败 Nack/requeue 并由任务管理器退避重试；无效消息拒绝进入死信。重复投递不会重复插入相同索引。扩散中断从第一批重试，本版尚未持久化逐批检查点，临近大 V 阈值的作者存在重复工作成本。
- 这是最终一致性关注流，不是冻结快照：异步消息迟到、关注关系改变期间，既有分页会话不能保证看到所有新进入的数据，刷新第一页或后续重建恢复。计数分类目前直接聚合关注表，高规模下可再引入可靠计数投影和活跃粉丝策略。

新分页协议（前端两个关注流入口均已接入）：

```json
{"limit":10,"cursor":""}
```

返回 `video_list`、`has_more`、`next_cursor` 和兼容字段 `next_time`。下一页原样传回不透明的 `next_cursor`，不要自行解析或使用视频秒级时间重新构造。仍接受旧的秒级 `latest_time`，但旧客户端没有同时间 ID 防漏保证；有 `cursor` 时优先使用，非法游标返回 400。

新增隔离测试覆盖同时间复合游标、多路去重、推/拉分流、重复投递、截断后历史回源、新关注回填、取关/删除过滤、类别切换、Redis 故障/驱逐，以及 Outbox 到两条时间线的真实 MQ 消费链路。

- `GET /healthz` 返回后端健康状态。
- 本地配置默认开启 pprof：API `localhost:6060`，Worker `localhost:6061`。
- 上传文件写入 `backend/.run/uploads`；Docker 环境挂载到 `backend_uploads` volume。
- Redis 用于 Token 缓存、视频实体缓存、Feed 时间线、热榜窗口、分片上传会话。
- RabbitMQ Topic Exchange 覆盖点赞、评论、关注、热度、视频时间线事件，并配置 DLX。

### 互动可靠性与缓存回填

点赞、评论采用以下链路：

```text
API → MySQL Outbox（持久化业务命令）→ RabbitMQ → 业务消费者
    → 事务：ConsumedEvent + 业务数据/计数 + 热度 Outbox
    → RabbitMQ → 热度消费者 → Redis Lua：去重 + 失效缓存 + 更新分钟桶
```

- 接口成功表示命令已持久化接受，不表示异步消费完成。RabbitMQ 暂时不可用时，命令留在 Outbox，恢复后由 Worker 投递；不再使用可能与不确定投递重复执行的直接写库回退。
- `consumed_events` 以消费者和 `event_id` 的哈希为唯一键，去重记录与业务写入同事务提交。记录不自动过期，删除记录会失去对应旧消息的重放保护。不同事件首次到达的乱序控制、客户端请求幂等键不在本协议内。
- Redis 用一个 Lua 脚本原子处理事件去重、缓存版本变更、详情/实体缓存删除和热度累加。Redis 失败返回消费者，不再吞错 ACK。去重标记和分钟桶按事件发生时间设置相同的固定两小时保留截止时间，过期事件不会重建热度桶。
- 两条视频缓存回填路径都在查询 MySQL 前读取 `video:generation:<id>`，回填时通过 Lua 比较版本。若期间发生失效，旧查询不能重新写入 Redis。版本键不设置 TTL；不要单独提前清理版本键或未到期的热度去重键。
- 本地实体缓存仍保留最多 5 秒的 TTL；热榜仍是最近 60 个分钟桶、合并快照保留 2 分钟。删除评论不扣分的既有业务规则保持不变。

迁移增加 `consumed_events` 表，并给 `outbox_msgs` 增加 `exchange`、`routing_key`、`payload` 列；API 的 AutoMigrate 已包含这些变更，独立 Worker 应在迁移完成后启动。原有视频发布 Outbox 仍可读取。

升级已有运行环境时，先暂停互动写入并处理完旧版业务及热度队列，再停旧 Worker、完成迁移并启动新版 API/Worker。不能让旧 Worker 消费新增的通用 Outbox，也不能把旧版已独立发送的热度与新版业务派生热度混合重放。此修复不会自动纠正旧版本已经产生的重复评论或错误累计计数。

新增回归测试覆盖：数据库中途失败和 Outbox 写入失败的事务回滚、点赞取消后的旧消息重放、评论去重及删除后重放、Redis 并发重复消费、Redis 错误传播、旧缓存回填拒绝、去重过期后的重放、并发发布确认和不可路由消息，以及 API 命令经 Outbox/MQ 到业务落库及 Redis 热度的完整链路。沿用上面的隔离集成测试命令；`TEST_MYSQL_DSN`、`TEST_AMQP_URL`、`TEST_REDIS_ADDR` 必须指向专用测试服务。