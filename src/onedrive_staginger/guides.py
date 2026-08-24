"""User-facing setup guides displayed by CLI help."""

AZURE_CLIENT_GUIDE = """
Azure 应用注册指南（设备代码授权）

1. 打开 https://entra.microsoft.com/ ，进入 App registrations，然后选择 New registration。
2. 填写任意应用名称。OneDrive Business 通常选择“仅此组织目录中的帐户”。
3. 创建后，在 Overview 页面复制 Application (client) ID 和 Directory (tenant) ID。
4. 打开 Authentication -> Advanced settings，将 Allow public client flows 设为 Yes。
5. 打开 API permissions，添加 Microsoft Graph 的 Delegated permissions：
   User.Read、Files.Read、offline_access。
6. 如果组织策略要求管理员同意，请由管理员执行 Grant admin consent。
7. 不要创建 Client Secret，也不需要配置 Redirect URI。
8. 在 CONFIG_DIR/config.yaml 中写入：
   azure:
     tenant_id: "Directory (tenant) ID"
     client_id: "Application (client) ID"
9. 运行 `onedrive-staginger CONFIG_DIR login`，按终端显示的网址和设备代码完成登录。
""".strip()
