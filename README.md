# 誰是臥底 Server 版

這個版本把房間狀態、角色、詞語、投票、淘汰、輪次全部放到 Node.js + Socket.IO 伺服器管理，不再依賴 Trystero/P2P。

## 本機執行

```bash
npm install
npm start
```

然後開啟：

```text
http://localhost:3000
```

同一個 Wi-Fi 的其他裝置可用電腦的區網 IP，例如：

```text
http://192.168.1.100:3000
```

Windows 可用 `ipconfig` 找 IPv4 Address。

## 部署

可部署到 Render、Railway、Fly.io、VPS 等支援 Node.js + WebSocket 的平台。純 GitHub Pages 不行，因為 GitHub Pages 只能放靜態檔案，不能執行 `server.js`。

## Server 版特性

- 房間狀態由伺服器統一保存
- 一人一票，送出後不能修改
- 只有所有仍在場玩家都投票後才結算
- 斷線不會自動淘汰
- 重新連線會用 token 接回原玩家身分
- 中途加入者為觀戰者
- 下一輪沿用同一組詞
- 新遊戲重新抽角色與詞組
