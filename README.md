# 📚 Welcome to Stockli

**语言 | Language**: [[中文说明](README.md) | English README](README.en.md)

---

## 🧩 项目简介

Stockli 是一个基于 Python 开发的个人量化研究与数据可视化工具。

- 💻 前端界面使用 **PySide6**
- 🗄️ 数据库后端采用 **PostgreSQL**
- 📡 接入免费金融数据 API，包括：
  - [LongPort 开放接口](https://open.longportapp.com/)
  - [Polygon.io](https://polygon.io/)

---

## 🚀 当前功能

### 1. 数据处理模块

- 数据库日线（OHLCV）数据增量更新  
- 自动处理以下情况：
  - 退市股票（Delisted）
  - IPO 检测
  - 并股、拆股（拆分）等事件

### 2. 图形界面

- 支持标的（股票代码）切换  
- 可视化显示交互式 K 线图（蜡烛图）  
- 使用 PySide6 构建桌面图形界面

### 3. AI Agent

- 集成LLM大模型接入LongPort MCP，实现NLP精准查询股票信息

---

## 📌 未来计划

- 特征工程与 alpha 信号可视化
- 集成预测模型
- 尝试构建 Web 版界面
- AI Agent接入定制MCP，沙盒化处理可视化策略和数据

---

## 📬

欢迎提交 issue 或通过 Pull Request 贡献代码。
