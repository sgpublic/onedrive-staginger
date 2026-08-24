# onedrive-staginger

`onedrive-staginger` 是一个将 OneDrive Business 文件单向迁移到本地磁盘的下载调度工具。它先将文件下载到容量受限的 `temp` 中转目录，完成大小和 hash 校验后，再搬运到 `dist` 最终目录，并保留所选 OneDrive 目录的相对路径。

它适合大容量迁移任务，不是双向同步工具，也不是 rclone 的替代品。

```text
OneDrive -> temp -> dist
```

下载、断点续传和重试由 aria2c 处理。SQLite 只保存 `sync` 获取的远端静态清单和 delta cursor；下载状态不持久化。下载与搬运中断后，重新执行下载命令会从本地文件重新校验并恢复。

## 使用

项目通过 [Pixi](https://pixi.sh/) 管理 Python 和全部依赖。下载命令首次运行时会将固定版本的 [Aria2-Pro-Core](https://github.com/P3TERX/Aria2-Pro-Core) 静态二进制下载到 `CONFIG_DIR/bin/aria2c`，后续强制使用该文件，不依赖系统或 conda 的 aria2c。先按 Pixi 官方文档安装 Pixi，再克隆本仓库：

```bash
git clone https://github.com/sgpublic/onedrive-staginger.git
cd onedrive-staginger
```

首次运行任意 `pixi run` 命令时，Pixi 会自动创建环境并安装依赖。

### 1. 配置 Azure 应用和调度参数

将示例配置复制为实际配置：

```bash
cp config/config.example.yaml config/config.yaml
```

在 Microsoft Entra 管理中心创建应用注册，并在 `config/config.yaml` 中填写 `tenant_id` 和 `client_id`。应用需要启用公共客户端流，并授予 Microsoft Graph 的委托权限：`User.Read`、`Files.Read` 和 `offline_access`。不需要创建 Client Secret 或 Redirect URI。

可按本机网络和磁盘性能调整 `scheduler`。机械盘还可调整 `aria2`：

```yaml
aria2:
  disk_cache: "256M"        # 所有下载共享的内存写入缓存
  file_allocation: "falloc" # ext4、XFS、btrfs 推荐，快速预分配大文件

scheduler:
  max_downloads: 2         # 同时下载的文件数
  max_moves: 1             # 同时从 temp 搬运到 dist 的文件数
  connections_per_file: 8  # 每个文件的 aria2 连接数
  min_split_size: "1M"     # aria2 分片阈值下限
  max_split_size: "4M"     # 按文件大小计算出的分片阈值上限
  fast_verify_after_download: false # 新下载文件仅按大小校验，默认仍校验哈希
```

`temp` 的容量由使用者自行控制；请根据可用空间设置 `max_downloads`。
`disk_cache` 默认值为 `16M`，设为 `0` 可禁用。缓存可减少机械盘随机写入，并让紧随下载的哈希校验复用内存中的数据。`falloc` 仅建议用于 ext4、XFS、btrfs 等支持快速预分配的文件系统；FAT32、ext3 或网络挂载目录请保留 `prealloc`。

### 2. 登录 OneDrive

执行设备代码登录，并根据终端提示在浏览器中完成授权：

```bash
pixi run login
```

登录信息会写入 `config/account.yaml`。该文件含有访问凭据，不应提交到版本库或泄露给他人。

### 3. 同步远端文件清单

下载前必须先同步。首次同步会读取整个 OneDrive，后续同步只获取远端变更：

```bash
pixi run sync
```

清单和同步游标保存在 `config/staging.sqlite`。

### 4. 下载指定目录

同步成功后，指定中转目录、最终目录和 OneDrive 中的根路径：

```bash
pixi run download /mnt/temp /mnt/dist /Media
```

其中：

- `/mnt/temp`：aria2 下载的中转目录。文件完成校验后会被删除。
- `/mnt/dist`：最终保存目录。
- `/Media`：OneDrive 中要下载的目录。最终目录会保留其内部的相对路径，例如 `/Media/Anime/01.mkv` 会保存为 `/mnt/dist/Anime/01.mkv`。

需要下载其他远端目录时，先重新执行 `sync`，再以新的 OneDrive 路径执行 `download`。远端删除不会自动删除 `dist` 中的文件。
